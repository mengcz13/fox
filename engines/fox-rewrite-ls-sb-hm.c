/*  - FOX - A tool for testing Open-Channel SSDs
 *      - Engine 5. Rewrite like log-structured file systems
 *
 * Copyright (C) 2016, IT University of Copenhagen. All rights reserved.
 * Written by Ivan Luiz Picoli <ivpi@itu.dk>
 *
 * Funding support provided by CAPES Foundation, Ministry of Education
 * of Brazil, Brasilia - DF 70040-020, Brazil.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *  this list of conditions and the following disclaimer in the documentation
 *  and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* ENGINE 7: Superblocks:
 * Read as usual;
 * Write like log-structured file systems;
 * Erase when garbage collection.
 * GC: always choose 
 * Written by Chuizheng Meng <mengcz13@mails.tsinghua.edu.cn>
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/queue.h>
#include "../fox.h"
#include "fox-rewrite-utils.h"

// entry for log block page map table
struct lbpm_entry {
    uint64_t vsblk_i;
    uint64_t psblk_i;
    uint64_t* vpg2ppg; // page-aligend mapping inside a superblock
};

struct sblkaddr {
    uint64_t offset_in_page;
    uint64_t pg_i;
    uint64_t inner_pu_i;
    uint64_t inner_blk_i;
    uint64_t outer_pu_i;
    uint64_t outer_blk_i;
};

struct logblockaddr {
    uint64_t offset_in_page;
    uint64_t insb_pg_i;
    uint64_t sblk_i;
};

/* Linked list for blocks */
struct sblk_meta {
    uint64_t ndirtypgs;
    uint64_t nabandonedpgs;
};

struct sblk_entry {
    uint64_t sblk_i;
    struct sblk_meta* meta;
    TAILQ_ENTRY(sblk_entry) pt;
};

TAILQ_HEAD(sblk_entry_list, sblk_entry);

struct sblk_list {
    struct sblk_entry_list empty_sblks;
    struct sblk_entry_list non_empty_sblks;
    struct sblk_entry* active_sblk;
};

struct ls_meta {
    uint64_t lbpm_entry_num;
    uint64_t sblk_npus; // number of pus for each superblock
    uint64_t sblk_nblks; // number of blk_i for each superblock, total blocks = sblk_npus * sblk_nblks
    uint64_t sblk_tblks; // total number of blocks in one superblock
    uint64_t sblk_tpgs; // total number of pages in one superblock
    uint64_t sblk_ntotal; // total number of superblocks
    struct rewrite_meta* meta;
    struct fox_blkbuf* blockbuf;
    uint64_t dirty_pg_count;
    uint64_t abandoned_pg_count;
    uint64_t clean_pg_count;
    uint64_t map_change_count;
    uint64_t map_set_count;
    uint64_t gc_count;
    uint64_t gc_time;
    uint64_t gc_map_change_count;
    uint64_t* vsblk2psblk;
    uint64_t* psblk2vsblk;
    struct lbpm_entry* lbpm; // log block page map table
    struct sblk_list* sblk_lists; // 1 for each mPU
    struct sblk_entry* sblk_entries;
    struct sblk_meta* sblk_metas; // storing meta info of blocks
    uint64_t next_mpu_i; // used to iterate over chs and luns
    uint8_t* sblkbuf;
    uint64_t* sblkvpgs;
};

static struct nodegeoaddr sblkaddr2geoaddr(struct ls_meta* lm, struct sblkaddr* sblka) {
    struct nodegeoaddr res;
    struct fox_node* node = lm->meta->node;
    res.offset_in_page = sblka->offset_in_page;
    res.pg_i = sblka->pg_i;
    res.blk_i = sblka->inner_blk_i + sblka->outer_blk_i * lm->sblk_nblks;
    uint64_t pui = sblka->inner_pu_i + sblka->outer_pu_i * lm->sblk_npus;
    res.lun_i = pui / node->nchs % node->nluns;
    res.ch_i = pui % node->nchs;
    return res;
}

static struct sblkaddr geoaddr2sblkaddr(struct ls_meta* lm, struct nodegeoaddr* geoaddr) {
    struct sblkaddr res;
    struct fox_node* node = lm->meta->node;
    res.offset_in_page = geoaddr->offset_in_page;
    res.pg_i = geoaddr->pg_i;
    res.outer_blk_i = geoaddr->blk_i / lm->sblk_nblks;
    res.inner_blk_i = geoaddr->blk_i % lm->sblk_nblks;
    uint64_t pui = geoaddr->lun_i * node->nchs + geoaddr->ch_i;
    res.outer_pu_i = pui / lm->sblk_npus;
    res.inner_pu_i = pui % lm->sblk_npus;
    return res;
}

static uint64_t sblkaddr2sblki(struct ls_meta* lm, struct sblkaddr* sblka) {
    struct fox_node* node = lm->meta->node;
    return sblka->outer_pu_i + sblka->outer_blk_i * (node->nchs * node->nluns / lm->sblk_npus);
}

static struct sblkaddr vpgi2sblkaddr(struct ls_meta* lm, uint64_t vpgi) {
    uint64_t w_ui = lm->sblk_npus;
    uint64_t w_pgs = lm->meta->node->npgs * w_ui;
    uint64_t w_bi = lm->sblk_nblks * w_pgs;
    uint64_t w_uo = (lm->meta->node->nchs * lm->meta->node->nluns / lm->sblk_npus) * w_bi;
    uint64_t w_bo = (lm->meta->node->nblks / lm->sblk_nblks) * w_uo;
    struct sblkaddr res;
    res.offset_in_page = 0;
    res.inner_pu_i = vpgi % w_ui;
    res.pg_i = vpgi / w_ui % (w_pgs / w_ui);
    res.inner_blk_i = vpgi / w_pgs % (w_bi / w_pgs);
    res.outer_pu_i = vpgi / w_bi % (w_uo / w_bi);
    res.outer_blk_i = vpgi / w_uo % (w_bo / w_uo);
    return res;
}

static uint64_t sblkaddr2vpgi(struct ls_meta* lm, struct sblkaddr* sblka) {
    uint64_t w_ui = lm->sblk_npus;
    uint64_t w_pgs = lm->meta->node->npgs * w_ui;
    uint64_t w_bi = lm->sblk_nblks * w_pgs;
    uint64_t w_uo = (lm->meta->node->nchs * lm->meta->node->nluns / lm->sblk_npus) * w_bi;
    // uint64_t w_bo = (lm->meta->node->nblks / lm->sblk_nblks) * w_uo;

    return sblka->inner_pu_i + sblka->pg_i * w_ui + sblka->inner_blk_i * w_pgs + sblka->outer_pu_i * w_bi + sblka->outer_blk_i * w_uo;
}

static uint64_t vpgi2sblki(struct ls_meta* lm, uint64_t vpgi) {
    struct sblkaddr temp = vpgi2sblkaddr(lm, vpgi);
    return sblkaddr2sblki(lm, &temp);
}

static struct sblkaddr sblki2sblkaddr(struct ls_meta* lm, uint64_t sblki) {
    struct sblkaddr res;
    res.offset_in_page = 0;
    res.pg_i = 0;
    uint64_t npuspblk = lm->meta->node->nchs * lm->meta->node->nluns / lm->sblk_npus;
    res.inner_pu_i = 0;
    res.outer_pu_i = sblki % npuspblk;
    res.inner_blk_i = 0;
    res.outer_blk_i = sblki / npuspblk;
    return res;
}

static struct logblockaddr sblkaddr2logblockaddr(struct ls_meta* lm, struct sblkaddr* sblka) {
    struct logblockaddr res;
    res.sblk_i = sblkaddr2sblki(lm, sblka);
    res.offset_in_page = sblka->offset_in_page;
    uint64_t w_ui = lm->sblk_npus;
    uint64_t w_pgs = lm->meta->node->npgs * w_ui;
    res.insb_pg_i = sblka->inner_pu_i + sblka->pg_i * w_ui + sblka->inner_blk_i * w_pgs;
    return res;
}

static struct sblkaddr logblockaddr2sblkaddr(struct ls_meta* lm, struct logblockaddr* logblocka) {
    struct sblkaddr res = sblki2sblkaddr(lm, logblocka->sblk_i);
    res.offset_in_page = logblocka->offset_in_page;
    uint64_t w_ui = lm->sblk_npus;
    uint64_t w_pgs = lm->meta->node->npgs * w_ui;
    res.inner_pu_i = logblocka->insb_pg_i % w_ui;
    res.pg_i = (logblocka->insb_pg_i / w_ui) % (w_pgs / w_ui);
    res.inner_blk_i = logblocka->insb_pg_i / w_pgs;
    return res;
}

static struct nodegeoaddr logblockaddr2geoaddr(struct ls_meta* lm, struct logblockaddr* logblocka) {
    struct sblkaddr midsblk = logblockaddr2sblkaddr(lm, logblocka);
    return sblkaddr2geoaddr(lm, &midsblk);
}

static struct nodegeoaddr vpg2geoaddr_sb(struct ls_meta* lm, uint64_t vpgi) {
    struct sblkaddr temp = vpgi2sblkaddr(lm, vpgi);
    return sblkaddr2geoaddr(lm, &temp);
}

static uint64_t geoaddr2vpg_sb(struct ls_meta* lm, struct nodegeoaddr* geoaddr) {
    struct sblkaddr temp = geoaddr2sblkaddr(lm, geoaddr);
    return sblkaddr2vpgi(lm, &temp);
}

static int init_ls_meta(struct rewrite_meta* meta, struct fox_blkbuf* blockbuf, struct ls_meta* lm) {
    uint64_t wl_npus = meta->node->wl->sb_pus;
    uint64_t wl_nblks = meta->node->wl->sb_blks;
    lm->lbpm_entry_num = 10;
    lm->sblk_npus = (wl_npus == 0) ? 1 : wl_npus;
    lm->sblk_nblks = (wl_nblks == 0) ? 1 : wl_nblks;
    printf("Superblock: %" PRId64 " PUs, %" PRId64 " BLKs\n", lm->sblk_npus, lm->sblk_nblks);
    lm->sblk_tblks = lm->sblk_npus * lm->sblk_nblks;
    lm->sblk_tpgs = lm->sblk_tblks * meta->node->npgs;
    printf("One superblock: %" PRId64 " blocks, %" PRId64 " pages\n", lm->sblk_tblks, lm->sblk_tpgs);
    lm->sblk_ntotal = (meta->node->nblks / lm->sblk_nblks) * (meta->node->nchs * meta->node->nluns / lm->sblk_npus);
    lm->meta = meta;
    lm->blockbuf = blockbuf;
    lm->dirty_pg_count = 0;
    lm->abandoned_pg_count = 0;
    lm->clean_pg_count = meta->total_pagenum;
    lm->map_change_count = 0;
    lm->map_set_count = 0;
    lm->gc_count = 0;
    lm->gc_time = 0;
    lm->gc_map_change_count = 0;
    lm->vsblk2psblk = (uint64_t*)calloc(lm->sblk_ntotal, sizeof(uint64_t));
    lm->psblk2vsblk = (uint64_t*)calloc(lm->sblk_ntotal, sizeof(uint64_t));
    lm->lbpm = (struct lbpm_entry*)calloc(lm->lbpm_entry_num, sizeof(struct lbpm_entry)); 
    lm->next_mpu_i = 0;
    lm->sblkbuf = (uint8_t*)calloc(lm->sblk_tblks * meta->node->npgs * meta->vpg_sz, sizeof(uint8_t));
    lm->sblkvpgs = (uint64_t*)calloc(lm->sblk_tblks * meta->node->npgs, sizeof(uint64_t));

    lm->sblk_metas = (struct sblk_meta*)calloc(lm->sblk_ntotal, sizeof(struct sblk_meta));
    lm->sblk_entries = (struct sblk_entry*)calloc(lm->sblk_ntotal, sizeof(struct sblk_entry));
    
    lm->sblk_lists = (struct sblk_list*)calloc(meta->node->nchs * meta->node->nluns / lm->sblk_npus, sizeof(struct sblk_list));
    struct sblkaddr tsblkaddr;
    int pui, blki;
    for (pui = 0; pui < meta->node->nchs * meta->node->nluns / lm->sblk_npus; pui++) {
        tsblkaddr.outer_pu_i = pui;
        struct sblk_list* listi = &(lm->sblk_lists[pui]);
        TAILQ_INIT(&(listi->empty_sblks));
        TAILQ_INIT(&(listi->non_empty_sblks));
        for (blki = 0; blki < meta->node->nblks / lm->sblk_nblks; blki++) {
            tsblkaddr.outer_blk_i = blki;
            uint64_t sblk_i = sblkaddr2sblki(lm, &tsblkaddr);
            struct sblk_entry* tsblk_entry = &(lm->sblk_entries[sblk_i]);
            tsblk_entry->sblk_i = sblk_i;
            tsblk_entry->meta = &(lm->sblk_metas[sblk_i]);
            TAILQ_INSERT_TAIL(&(listi->empty_sblks), tsblk_entry, pt);
        }
        listi->active_sblk = NULL;
    }

    int vsbi;
    for (vsbi = 0; vsbi < lm->sblk_ntotal; vsbi++) {
        lm->vsblk2psblk[vsbi] = lm->sblk_ntotal;
    }
    int vpbi;
    for (vpbi = 0; vpbi < lm->sblk_ntotal; vpbi++) {
        lm->psblk2vsblk[vpbi] = lm->sblk_ntotal;
    }
    int lbpmi;
    for (lbpmi = 0; lbpmi < lm->lbpm_entry_num; lbpmi++) {
        struct lbpm_entry* le = &(lm->lbpm[lbpmi]);
        le->vsblk_i = lm->sblk_ntotal;
        le->psblk_i = lm->sblk_ntotal;
        le->vpg2ppg = (uint64_t*)calloc(lm->sblk_tpgs, sizeof(uint64_t));
        int lbpmi_pgi;
        for (lbpmi_pgi = 0; lbpmi_pgi < lm->sblk_tpgs; lbpmi_pgi++)
            le->vpg2ppg[lbpmi_pgi] = lm->sblk_tpgs;
    }
    return 0;
}

static int free_ls_meta(struct ls_meta* lm) {
    free(lm->vsblk2psblk);
    free(lm->psblk2vsblk);
    free(lm->sblk_metas);
    free(lm->sblk_entries);
    free(lm->sblk_lists);
    free(lm->sblkbuf);
    free(lm->sblkvpgs);
    int lbpmi;
    for (lbpmi = 0; lbpmi < lm->lbpm_entry_num; lbpmi++)
        free(lm->lbpm[lbpmi].vpg2ppg);
    free(lm->lbpm);
    return 0;
}

static uint64_t vsblk2psblk(struct ls_meta* lm, uint64_t vsbi) {
    return lm->vsblk2psblk[vsbi];
}

static uint64_t psblk2vsblk(struct ls_meta* lm, uint64_t psbi) {
    return lm->psblk2vsblk[psbi];
}

static struct nodegeoaddr vaddr2paddr(struct ls_meta* lm, struct nodegeoaddr* vaddr) {
    struct sblkaddr vsblkaddr = geoaddr2sblkaddr(lm, vaddr);
    struct logblockaddr vlogblockaddr = sblkaddr2logblockaddr(lm, &vsblkaddr);
    uint64_t vsblki = vlogblockaddr.sblk_i;
    // first search in log blocks
    int lbpmi;
    struct logblockaddr plogblockaddr;
    plogblockaddr.sblk_i = lm->sblk_ntotal;
    plogblockaddr.insb_pg_i = lm->sblk_tpgs;
    plogblockaddr.offset_in_page = vlogblockaddr.offset_in_page;
    for (lbpmi = 0; lbpmi < lm->lbpm_entry_num; lbpmi++) {
        if (lm->lbpm[lbpmi].vsblk_i == vsblki) {
            uint64_t pinsb_pg_i = lm->lbpm[lbpmi].vpg2ppg[vlogblockaddr.insb_pg_i];
            if (pinsb_pg_i < lm->sblk_tpgs) {
                plogblockaddr.sblk_i = lm->lbpm[lbpmi].psblk_i;
                plogblockaddr.insb_pg_i = pinsb_pg_i;
                struct sblkaddr psblkaddr = logblockaddr2sblkaddr(lm, &plogblockaddr);
                return sblkaddr2geoaddr(lm, &psblkaddr);
            }
        }
    }
    // then search in data blocks
    uint64_t psblki = vsblk2psblk(lm, vsblki);
    struct sblkaddr psblkaddr = sblki2sblkaddr(lm, psblki);
    psblkaddr.inner_blk_i = vsblkaddr.inner_blk_i;
    psblkaddr.inner_pu_i = vsblkaddr.inner_pu_i;
    psblkaddr.offset_in_page = vsblkaddr.offset_in_page;
    psblkaddr.pg_i = vsblkaddr.pg_i;
    struct nodegeoaddr paddr = sblkaddr2geoaddr(lm, &psblkaddr);
    return paddr;
}

static uint64_t vpg2ppg(struct ls_meta* lm, uint64_t vpg) {
    struct nodegeoaddr vaddr = vpg2geoaddr_sb(lm, vpg);
    struct nodegeoaddr paddr = vaddr2paddr(lm, &vaddr);
    return geoaddr2vpg_sb(lm, &paddr);
}

/*
static int isalloc_sbi(struct ls_meta* lm, uint64_t vsbi) {
    if (vsblk2psblk(lm, vsbi) == lm->sblk_ntotal)
        return 0;
    else
        return 1;
}*/

static int isalloc(struct ls_meta* lm, uint64_t vpgi) {
    struct sblkaddr vpg_sblkaddr = vpgi2sblkaddr(lm, vpgi);
    struct logblockaddr vpg_logblockaddr = sblkaddr2logblockaddr(lm, &vpg_sblkaddr);
    uint64_t vsblki = vpg_logblockaddr.sblk_i;
    // first search in log blocks
    int lbpmi;
    for (lbpmi = 0; lbpmi < lm->lbpm_entry_num; lbpmi++) {
        if (lm->lbpm[lbpmi].vsblk_i == vsblki) {
            uint64_t pinsb_pg_i = lm->lbpm[lbpmi].vpg2ppg[vpg_logblockaddr.insb_pg_i];
            if (pinsb_pg_i < lm->sblk_tpgs) {
                return 1;
            }
        }
    }
    // then search in data blocks
    uint64_t psblki = vsblk2psblk(lm, vsblki);
    if (psblki == lm->sblk_ntotal)
        return 0;
    else {
        uint64_t ppgi = vpg2ppg(lm, vpgi);
        if (lm->meta->page_state[ppgi] == PAGE_CLEAN)
            return 0;
        else
            return 1;
    }
}

static struct sblk_entry* find_next_free_sb(struct ls_meta* lm);

static struct sblk_entry* gc_until_find_next_free_sb(struct ls_meta* lm);

static int abandon_sblk(struct ls_meta* lm, uint64_t data_psblk_i) {
    struct sblk_entry* olddb = &(lm->sblk_entries[data_psblk_i]);
    uint64_t olddirty = olddb->meta->ndirtypgs;
    olddb->meta->ndirtypgs = 0;
    olddb->meta->nabandonedpgs = olddirty;
    lm->dirty_pg_count -= olddirty;
    lm->abandoned_pg_count += olddirty;
    lm->psblk2vsblk[data_psblk_i] = lm->sblk_ntotal;
    return 0;
}

static int check_datafit(struct ls_meta* lm, struct lbpm_entry* le) {
    uint64_t pginsbi;
    int datafit = 1;
    for (pginsbi = 0; pginsbi < lm->sblk_tpgs; pginsbi++) {
        if (le->vpg2ppg[pginsbi] != pginsbi) {
            datafit = 0;
            break;
        }
    }
    return datafit;
}

static int merge_cost(struct ls_meta* lm, struct lbpm_entry* le) {
    int datafit = check_datafit(lm, le);
    if (datafit == 1)
        return 0;
    else
        return lm->sblk_tpgs;
}

static int rw_inside_page_sb(struct ls_meta* lm, struct fox_blkbuf* blockbuf, uint8_t* databuf, struct rewrite_meta* meta, struct nodegeoaddr* geoaddr, uint64_t size, int mode);

static int merge_log_data(struct ls_meta* lm, uint64_t vsblk_i) {
    // merge log block and data block of vsblk_i
    // there must be one mapping in log block page mapping!
    uint64_t lbpmi;
    struct lbpm_entry* le = NULL;
    for (lbpmi = 0; lbpmi < lm->lbpm_entry_num; lbpmi++) {
        if (lm->lbpm[lbpmi].vsblk_i == vsblk_i) {
            le = &(lm->lbpm[lbpmi]);
            break;
        }
    }
    uint64_t log_psblk_i = le->psblk_i;
    // check if log block can be directly used as data block
    int datafit = check_datafit(lm, le);
    // get data block
    uint64_t data_psblk_i = lm->vsblk2psblk[vsblk_i];
    if (datafit) {
        // use log block directly as data block!
        if (data_psblk_i != lm->sblk_ntotal) {
            abandon_sblk(lm, data_psblk_i);
        }
        lm->vsblk2psblk[vsblk_i] = log_psblk_i;
        lm->psblk2vsblk[log_psblk_i] = vsblk_i;
        if (data_psblk_i != lm->sblk_ntotal)
            lm->map_change_count++;
        else
            lm->map_set_count++;
    } else {
        struct sblk_entry* newsblk = gc_until_find_next_free_sb(lm);
        uint64_t insbpgi;
        struct logblockaddr t_logblk, t_datablk, t_targetblk;
        t_logblk.offset_in_page = t_datablk.offset_in_page = t_targetblk.offset_in_page = 0;
        t_logblk.sblk_i = log_psblk_i;
        t_datablk.sblk_i = data_psblk_i;
        t_targetblk.sblk_i = newsblk->sblk_i;
        for (insbpgi = 0; insbpgi < lm->sblk_tpgs; insbpgi++) {
            t_datablk.insb_pg_i = t_targetblk.insb_pg_i = insbpgi;
            struct nodegeoaddr srcaddr;
            struct nodegeoaddr dstaddr = logblockaddr2geoaddr(lm, &t_targetblk);
            if (le->vpg2ppg[insbpgi] < lm->sblk_tpgs) {
                t_logblk.insb_pg_i = le->vpg2ppg[insbpgi];
                srcaddr = logblockaddr2geoaddr(lm, &t_logblk);
                rw_inside_page_sb(lm, lm->blockbuf, lm->meta->pagebuf, lm->meta, &srcaddr, lm->meta->vpg_sz, READ_MODE);
                rw_inside_page_sb(lm, lm->blockbuf, lm->meta->pagebuf, lm->meta, &dstaddr, lm->meta->vpg_sz, WRITE_MODE);
            } else if (data_psblk_i != lm->sblk_ntotal) {
                srcaddr = logblockaddr2geoaddr(lm, &t_datablk);
                if (lm->meta->page_state[geoaddr2vpg_sb(lm, &srcaddr)] == PAGE_DIRTY) {
                    rw_inside_page_sb(lm, lm->blockbuf, lm->meta->pagebuf, lm->meta, &srcaddr, lm->meta->vpg_sz, READ_MODE);
                    rw_inside_page_sb(lm, lm->blockbuf, lm->meta->pagebuf, lm->meta, &dstaddr, lm->meta->vpg_sz, WRITE_MODE);
                }
            }
        }
        // abandon log block and data block (if exists)
        lm->vsblk2psblk[vsblk_i] = newsblk->sblk_i;
        lm->psblk2vsblk[newsblk->sblk_i] = vsblk_i;
        if (data_psblk_i != lm->sblk_ntotal) {
            abandon_sblk(lm, data_psblk_i);
            lm->map_change_count++;
        } else {
            lm->map_set_count++;
        }
        abandon_sblk(lm, log_psblk_i);
    }
    // clean log table
    le->vsblk_i = lm->sblk_ntotal;
    le->psblk_i = lm->sblk_ntotal;
    int pginsbi;
    for (pginsbi = 0; pginsbi < lm->sblk_tpgs; pginsbi++) {
        le->vpg2ppg[pginsbi] = lm->sblk_tpgs;
    }
    return 0;
}

static uint64_t alloc_page(struct ls_meta* lm, uint64_t vpgi, uint64_t vpgi_begin, uint64_t vpgi_end) {
    struct sblkaddr vsblkaddr = vpgi2sblkaddr(lm, vpgi);
    struct logblockaddr vlogblockaddr = sblkaddr2logblockaddr(lm, &vsblkaddr);
    struct lbpm_entry* match = NULL;
    struct lbpm_entry* avail = NULL;
    int lbpmi;
    for (lbpmi = 0; lbpmi < lm->lbpm_entry_num; lbpmi++) {
        if (lm->lbpm[lbpmi].vsblk_i == vlogblockaddr.sblk_i) {
            match = &(lm->lbpm[lbpmi]);
        }
        if (lm->lbpm[lbpmi].vsblk_i == lm->sblk_ntotal) {
            avail = &(lm->lbpm[lbpmi]);
        }
    }
    if (match == NULL) {
        // set a new match!
        if (avail == NULL) {
            // find one to merge
            int min_cost = (1 << 30);
            struct lbpm_entry* to_merge = NULL;
            for (lbpmi = 0; lbpmi < lm->lbpm_entry_num; lbpmi++) {
                int costi = merge_cost(lm, &(lm->lbpm[lbpmi]));
                if (costi < min_cost) {
                    min_cost = costi;
                    to_merge = &(lm->lbpm[lbpmi]);
                }
            }
            merge_log_data(lm, to_merge->vsblk_i);
            avail = to_merge;
            lm->map_set_count += 2;
        } else {
            lm->map_change_count += 2;
        }
        struct sblk_entry* newlogblk = gc_until_find_next_free_sb(lm);
        avail->vsblk_i = vlogblockaddr.sblk_i;
        avail->psblk_i = newlogblk->sblk_i;
        match = avail;
    }
    // if (match != NULL) {
    if (1) {
        // merge current page if full
        struct sblk_entry* logblk = &(lm->sblk_entries[match->psblk_i]);
        if (logblk->meta->ndirtypgs + logblk->meta->nabandonedpgs == lm->sblk_tpgs) {
            merge_log_data(lm, match->vsblk_i);
            logblk = gc_until_find_next_free_sb(lm);
            match->vsblk_i = vlogblockaddr.sblk_i;
            match->psblk_i = logblk->sblk_i;
            lm->map_change_count += 2;
        }
        // write new page
        uint64_t psblk_i = logblk->sblk_i;
        uint64_t newpgid = logblk->meta->ndirtypgs + logblk->meta->nabandonedpgs;
        struct logblockaddr newpg;
        newpg.sblk_i = psblk_i;
        newpg.insb_pg_i = newpgid;
        newpg.offset_in_page = 0;
        struct sblkaddr newpg_sblkaddr = logblockaddr2sblkaddr(lm, &newpg);
        uint64_t newppg_i = sblkaddr2vpgi(lm, &newpg_sblkaddr);
        uint64_t oldpgmap = match->vpg2ppg[vlogblockaddr.insb_pg_i];
        if (oldpgmap == lm->sblk_tpgs) {
            match->vpg2ppg[vlogblockaddr.insb_pg_i] = newpgid;
            logblk->meta->ndirtypgs++;
            lm->dirty_pg_count++;
            lm->clean_pg_count--;
            lm->map_set_count++;
        } else {
            struct logblockaddr oldmappage;
            oldmappage.sblk_i = psblk_i;
            oldmappage.insb_pg_i = oldpgmap;
            oldmappage.offset_in_page = 0;
            struct nodegeoaddr oldmappage_geoaddr = logblockaddr2geoaddr(lm, &oldmappage);
            lm->meta->page_state[geoaddr2vpg_sb(lm, &oldmappage_geoaddr)] = PAGE_ABANDONED;
            match->vpg2ppg[vlogblockaddr.insb_pg_i] = newpgid;
            logblk->meta->nabandonedpgs++;
            lm->abandoned_pg_count++;
            lm->clean_pg_count--;
            lm->map_change_count++;
        }
        return newppg_i;
    }
}

static int rw_inside_page_sb(struct ls_meta* lm, struct fox_blkbuf* blockbuf, uint8_t* databuf, struct rewrite_meta* meta, struct nodegeoaddr* geoaddr, uint64_t size, int mode) {
    struct fox_node* node = lm->meta->node;
    uint64_t ch_i = geoaddr->ch_i;
    uint64_t lun_i = geoaddr->lun_i;
    uint64_t blk_i = geoaddr->blk_i;
    uint64_t pg_i = geoaddr->pg_i;
    uint64_t offset = geoaddr->offset_in_page;
    uint64_t vpgofgeoaddr = geoaddr2vpg_sb(lm, geoaddr);
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    if (offset >= vpg_sz || offset + size > vpg_sz) {
        return 1;
    }
    if (mode == READ_MODE) {
        if (fox_read_blk(&node->vblk_tgt, node, blockbuf, 1, pg_i)) {
            return 1;
        }
        meta->heatmap[vpgofgeoaddr].readt++;
        memcpy(databuf, blockbuf->buf_r + vpg_sz * pg_i + offset, size);
    } else if (mode == WRITE_MODE) {
        uint8_t* pgst = &(meta->page_state[vpgofgeoaddr]);
        if (*pgst == PAGE_DIRTY) {
            printf("Writing to dirty page!\n");
            printf("%" PRIu64 ", %" PRIu64 ", %" PRIu64 ", %" PRIu64 ", %" PRIu64 ", %d\n", ch_i, lun_i, blk_i, pg_i, offset, mode);
            return 1;
        } else if (offset == 0 && size == vpg_sz) {
            memcpy(blockbuf->buf_w + vpg_sz * pg_i, databuf, size);
            if (fox_write_blk(&node->vblk_tgt, node, blockbuf, 1, pg_i)) {
                return 1;
            }
            meta->heatmap[vpgofgeoaddr].writet++;
        } else {
            // cannot rewrite before erasing!
            return 1;
        }
        *pgst = PAGE_DIRTY;
        uint8_t* blkst = get_p_blk_state(meta, geoaddr);
        *blkst = BLOCK_DIRTY;
    }
    return 0;
}

static int erase_block_sb(struct ls_meta* lm, struct rewrite_meta* meta, struct nodegeoaddr* geoaddr) {
    struct fox_node* node = lm->meta->node;
    uint64_t ch_i = geoaddr->ch_i;
    uint64_t lun_i = geoaddr->lun_i;
    uint64_t blk_i = geoaddr->blk_i;
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    if (fox_erase_blk(&node->vblk_tgt, node))
        return 1;
    *get_p_blk_state(meta, geoaddr) = BLOCK_CLEAN;
    struct nodegeoaddr tgeo = *geoaddr;
    for (tgeo.pg_i = 0; tgeo.pg_i < node->npgs; tgeo.pg_i++) {
        uint64_t vpgoftgeo = geoaddr2vpg_sb(lm, &tgeo);
        meta->page_state[vpgoftgeo] = PAGE_CLEAN;
        meta->heatmap[vpgoftgeo].eraset++;
    }
    return 0;
}

static int erase_sb(struct ls_meta* lm, uint64_t psblki);

static int garbage_collection(struct ls_meta* lm) {
    struct timeval tvalst, tvaled;
    gettimeofday(&tvalst, NULL);
    int offset = 0;
    int recycleflag = 0;
    uint64_t total_mpus = lm->meta->node->nchs * lm->meta->node->nluns / lm->sblk_npus;
    for (offset = 0; offset < total_mpus; offset++) {
        struct sblk_entry_list* nonemptylist = &(lm->sblk_lists[lm->next_mpu_i].non_empty_sblks);
        if (!TAILQ_EMPTY(nonemptylist)) {
            struct sblk_entry* iter = TAILQ_FIRST(nonemptylist);
            while (iter != NULL) {
                struct sblk_entry* iternext = TAILQ_NEXT(iter, pt);
                uint64_t psblki = iter->sblk_i;
                if (iter->meta->ndirtypgs == 0) {
                    erase_sb(lm, psblki);
                    TAILQ_REMOVE(nonemptylist, iter, pt);
                    TAILQ_INSERT_TAIL(&(lm->sblk_lists[lm->next_mpu_i].empty_sblks), iter, pt);
                    lm->abandoned_pg_count -= iter->meta->nabandonedpgs;
                    lm->clean_pg_count += iter->meta->nabandonedpgs;
                    iter->meta->nabandonedpgs = 0;
                    recycleflag++;
                    break;
                }
                iter = iternext;
            }
            if (recycleflag > 0)
                break;
        }
        lm->next_mpu_i = (lm->next_mpu_i + 1) % total_mpus;
    }
    gettimeofday(&tvaled, NULL);
    lm->gc_time += ((uint64_t)(tvaled.tv_sec - tvalst.tv_sec) * 1000000L + tvaled.tv_usec) - tvalst.tv_usec;
    lm->gc_count++;
    return 0;
}

static struct sblk_entry* find_next_free_sb(struct ls_meta* lm) {
    int offset = 0;
    uint64_t total_mpus = lm->meta->node->nchs * lm->meta->node->nluns / lm->sblk_npus;
    for (offset = 0; offset < total_mpus; offset++) {
        struct sblk_entry_list* emptylist = &(lm->sblk_lists[lm->next_mpu_i].empty_sblks);
        if (!TAILQ_EMPTY(emptylist)) {
            lm->next_mpu_i = (lm->next_mpu_i + 1) % total_mpus;
            struct sblk_entry* res = TAILQ_FIRST(emptylist);
            TAILQ_REMOVE(emptylist, res, pt);
            TAILQ_INSERT_TAIL(&(lm->sblk_lists[lm->next_mpu_i].non_empty_sblks), res, pt);
            return res;
        }
        lm->next_mpu_i = (lm->next_mpu_i + 1) % total_mpus;
    }
    return NULL;
}

static struct sblk_entry* gc_until_find_next_free_sb(struct ls_meta* lm) {
    struct sblk_entry* res = find_next_free_sb(lm);
    while (res == NULL) {
        garbage_collection(lm);
        res = find_next_free_sb(lm);
    }
    return res;
}

static int erase_sb(struct ls_meta* lm, uint64_t psblki) {
    uint64_t inner_pui, inner_blki;
    struct sblkaddr ta = sblki2sblkaddr(lm, psblki);
    ta.offset_in_page = 0;
    for (inner_blki = 0; inner_blki < lm->sblk_nblks; inner_blki++) {
        for (inner_pui = 0; inner_pui < lm->sblk_npus; inner_pui++) {
            ta.inner_blk_i = inner_blki;
            ta.inner_pu_i = inner_pui;
            ta.pg_i = 0;
            struct nodegeoaddr spgeo = sblkaddr2geoaddr(lm, &ta);
            if (*get_p_blk_state(lm->meta, &spgeo) == BLOCK_DIRTY) {
                erase_block_sb(lm, lm->meta, &spgeo);
            }
        }
    }
    return 0;
}

static int iterate_ls_io(struct fox_node* node, struct fox_blkbuf* buf, struct rewrite_meta* meta, struct ls_meta* lm, uint8_t* resbuf, uint64_t offset, uint64_t size, int mode) {
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    struct nodegeoaddr voffset_begin;
    struct nodegeoaddr voffset_end;
    set_nodegeoaddr(node, &voffset_begin, offset);
    set_nodegeoaddr(node, &voffset_end, offset + size - 1); // [offset_begin, offset_end]
    struct nodegeoaddr poffset_begin = vaddr2paddr(lm, &voffset_begin);
    struct nodegeoaddr poffset_end = vaddr2paddr(lm, &voffset_end);
    uint64_t vpg_i_begin = offset / vpg_sz;
    uint64_t vpg_i_end = (offset + size - 1) / vpg_sz;

    if (mode == READ_MODE) {
        struct nodegeoaddr vpg_geo_end = vpg2geoaddr_sb(lm, vpg_i_end);
        struct nodegeoaddr ppg_geo_end = vaddr2paddr(lm, &vpg_geo_end);
        uint8_t* resbuf_t = resbuf;
        // read
        if (vpg_i_begin == vpg_i_end) {
            rw_inside_page_sb(lm, buf, resbuf_t, meta, &poffset_begin, size, mode);
            resbuf_t += size;
        } else {
            // read begin page
            rw_inside_page_sb(lm, buf, resbuf_t, meta, &poffset_begin, vpg_sz - poffset_begin.offset_in_page, mode);
            resbuf_t += (vpg_sz - poffset_begin.offset_in_page);
            // read middle pages
            if (vpg_i_end - vpg_i_begin > 1) {
                uint64_t middle_pgi;
                for (middle_pgi = vpg_i_begin + 1; middle_pgi < vpg_i_end; middle_pgi++) {
                    struct nodegeoaddr tvgeo = vpg2geoaddr_sb(lm, middle_pgi);
                    struct nodegeoaddr tgeo = vaddr2paddr(lm, &tvgeo);
                    rw_inside_page_sb(lm, buf, resbuf_t, meta, &tgeo, vpg_sz, mode);
                    resbuf_t += vpg_sz;
                }
            }
            // read end page
            rw_inside_page_sb(lm, buf, resbuf_t, meta, &ppg_geo_end, poffset_end.offset_in_page + 1, mode);
            resbuf_t += (poffset_end.offset_in_page + 1);
        }
    } else if (mode == WRITE_MODE) {
        struct nodegeoaddr vpg_geo_begin = vpg2geoaddr_sb(lm, vpg_i_begin);
        struct nodegeoaddr vpg_geo_end = vpg2geoaddr_sb(lm, vpg_i_end);
        struct nodegeoaddr ppg_geo_begin = vaddr2paddr(lm, &vpg_geo_begin);
        struct nodegeoaddr ppg_geo_end = vaddr2paddr(lm, &vpg_geo_end);
        uint8_t* resbuf_t = resbuf;
        // read first and last pages (if necessary) before GC!
        if (isalloc(lm, vpg_i_begin) && (((vpg_i_begin == vpg_i_end) && (voffset_begin.offset_in_page != 0 || voffset_end.offset_in_page != vpg_sz - 1)) || ((vpg_i_begin < vpg_i_end) && (voffset_begin.offset_in_page != 0))))
            rw_inside_page_sb(lm, buf, meta->begin_pagebuf, meta, &ppg_geo_begin, vpg_sz, READ_MODE);
        if (isalloc(lm, vpg_i_end) && ((vpg_i_begin < vpg_i_end) && (voffset_end.offset_in_page != vpg_sz - 1)))
            rw_inside_page_sb(lm, buf, meta->end_pagebuf, meta, &ppg_geo_end, vpg_sz, READ_MODE);
        
        /*while (lm->clean_pg_count < vpg_i_end - vpg_i_begin + 1) {
            garbage_collection(lm);
        }
        realloc_sb(lm, vpg_i_begin, vpg_i_end);*/
        
        // read or write...
        if (vpg_i_begin == vpg_i_end) {
            if (isalloc(lm, vpg_i_begin) && (voffset_begin.offset_in_page != 0 || voffset_end.offset_in_page != vpg_sz - 1)) {
            }
            // alloc a new page here
            uint64_t newppg = alloc_page(lm, vpg_i_begin, vpg_i_begin, vpg_i_end);
            struct nodegeoaddr newppgaddr = vpg2geoaddr_sb(lm, newppg);
            memcpy(meta->begin_pagebuf + voffset_begin.offset_in_page, resbuf_t, size);
            rw_inside_page_sb(lm, buf, meta->begin_pagebuf, meta, &newppgaddr, vpg_sz, mode);
            resbuf_t += size;
        } else {
            // rw begin page
            if (isalloc(lm, vpg_i_begin) && (voffset_begin.offset_in_page != 0)) {
            }
            uint64_t newppg = alloc_page(lm, vpg_i_begin, vpg_i_begin, vpg_i_end);
            struct nodegeoaddr newppgaddr = vpg2geoaddr_sb(lm, newppg);
            memcpy(meta->begin_pagebuf + voffset_begin.offset_in_page, resbuf_t, vpg_sz - voffset_begin.offset_in_page);
            rw_inside_page_sb(lm, buf, meta->begin_pagebuf, meta, &newppgaddr, vpg_sz, mode);
            resbuf_t += (vpg_sz - voffset_begin.offset_in_page);
            // rw middle pages
            if (vpg_i_end - vpg_i_begin > 1) {
                uint64_t middle_pgi;
                for (middle_pgi = vpg_i_begin + 1; middle_pgi < vpg_i_end; middle_pgi++) {
                    newppg = alloc_page(lm, middle_pgi, 1, 0);
                    newppgaddr = vpg2geoaddr_sb(lm, newppg);
                    rw_inside_page_sb(lm, buf, resbuf_t, meta, &newppgaddr, vpg_sz, mode);
                    resbuf_t += vpg_sz;
                }
            }
            // rw end page
            if (isalloc(lm, vpg_i_end) && (voffset_end.offset_in_page != vpg_sz - 1)) {
            }
            newppg = alloc_page(lm, vpg_i_end, 1, 0);
            newppgaddr = vpg2geoaddr_sb(lm, newppg);
            memcpy(meta->end_pagebuf, resbuf_t, voffset_end.offset_in_page + 1);
            rw_inside_page_sb(lm, buf, meta->end_pagebuf, meta, &newppgaddr, vpg_sz, mode);
            resbuf_t += (voffset_end.offset_in_page + 1);
        }
    }
    return 0;
}

static int rewrite_ls_start (struct fox_node *node)
{
    node->stats.pgs_done = 0;
    struct fox_blkbuf nbuf;

    if (fox_alloc_blk_buf (node, &nbuf))
        goto OUT;

    struct rewrite_meta meta;
    init_rewrite_meta(node, &meta);
    struct ls_meta lm;
    init_ls_meta(&meta, &nbuf, &lm);

    uint64_t max_iosize = 0;
    uint64_t t = 0;
    for (t = 0; t < meta.ioseqlen; t++) {
        if (meta.ioseq[t].size > max_iosize)
            max_iosize = meta.ioseq[t].size;
    }
    uint8_t* databuf = (uint8_t*)calloc(max_iosize, sizeof(uint8_t));
    struct timeval tvalst, tvaled;

    fox_start_node (node);

    for (t = 0; t < meta.ioseqlen; t++) {
        if (t % 100 == 0) {
            printf("%d/%d\n", t, meta.ioseqlen);
        }
        int mode;
        if (meta.ioseq[t].iotype == 'r')
            mode = READ_MODE;
        else if (meta.ioseq[t].iotype == 'w')
            mode = WRITE_MODE;

        gettimeofday(&tvalst, NULL);
        iterate_ls_io(node, &nbuf, &meta, &lm, databuf, meta.ioseq[t].offset, meta.ioseq[t].size, mode);
        gettimeofday(&tvaled, NULL);
        // record time
        meta.ioseq[t].exetime = ((uint64_t)(tvaled.tv_sec - tvalst.tv_sec) * 1000000L + tvaled.tv_usec) - tvalst.tv_usec;
        // record benefit / cost
        meta.ioseq[t].nabandoned = lm.abandoned_pg_count;
        meta.ioseq[t].ndirty = lm.dirty_pg_count;
        meta.ioseq[t].map_change_count = lm.map_change_count;
        meta.ioseq[t].map_set_count = lm.map_set_count;
        meta.ioseq[t].gc_count = lm.gc_count;
        meta.ioseq[t].gc_time = lm.gc_time;
        meta.ioseq[t].gc_map_change_count = lm.gc_map_change_count;
        struct fox_stats* st = &node->stats;
        meta.ioseq[t].bread = st->bread;
        meta.ioseq[t].pgs_r = st->pgs_r;
        meta.ioseq[t].bwritten = st->bwritten;
        meta.ioseq[t].pgs_w = st->pgs_w;
        meta.ioseq[t].erased_blks = st->erased_blks;
        meta.ioseq[t].erase_t = st->erase_t;
        meta.ioseq[t].read_t = st->read_t;
        meta.ioseq[t].write_t = st->write_t;
    }
    fox_end_node (node);

    write_meta_stats(&meta);

    fox_free_blkbuf (&nbuf, 1);
    free(databuf);
    free_rewrite_meta(&meta);
    free_ls_meta(&lm);
    printf("\n[%" PRId64 ", %" PRId64 "]\n", lm.map_change_count, lm.map_set_count);
    return 0;

OUT:
    return -1;
}

static void rewrite_ls_exit (void)
{
    return;
}

static struct fox_engine rewrite_ls_sb_hm_engine = {
    .id             = FOX_ENGINE_8,
    .name           = "rewrite_ls_sb_hm",
    .start          = rewrite_ls_start,
    .exit           = rewrite_ls_exit,
};

int foxeng_rewrite_ls_sb_hm_init (struct fox_workload *wl)
{
    return fox_engine_register(&rewrite_ls_sb_hm_engine);
}
