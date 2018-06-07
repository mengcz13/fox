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

/* ENGINE 6: Rewrite sequences like log-structured file systems:
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

/* Linked list for blocks */
struct blk_meta {
    uint64_t ndirtypgs;
    uint64_t nabandonedpgs;
};

struct blk_entry {
    uint64_t pblk_i;
    struct blk_meta* meta;
    TAILQ_ENTRY(blk_entry) pt;
};

TAILQ_HEAD(blk_entry_list, blk_entry);

struct blk_list {
    struct blk_entry_list empty_blks;
    struct blk_entry_list non_empty_blks;
    struct blk_entry* active_blk;
};

struct ls_meta {
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
    uint64_t* vpg2ppg;
    uint64_t* ppg2vpg;
    struct blk_list* blk_lists; // 1 for each PU
    struct blk_entry* blk_entries;
    struct blk_meta* blk_metas; // storing meta info of blocks
    uint64_t next_ch_lun_i; // used to iterate over chs and luns
    uint8_t* blkbuf;
    uint64_t* blkvpgs;
};

static int init_ls_meta(struct rewrite_meta* meta, struct fox_blkbuf* blockbuf, struct ls_meta* lm) {
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
    lm->vpg2ppg = (uint64_t*)calloc(meta->total_pagenum, sizeof(uint64_t));
    lm->ppg2vpg = (uint64_t*)calloc(meta->total_pagenum, sizeof(uint64_t));
    lm->next_ch_lun_i = 0;
    lm->blkbuf = (uint8_t*)calloc(meta->node->npgs * meta->vpg_sz, sizeof(uint8_t));
    lm->blkvpgs = (uint64_t*)calloc(meta->node->npgs, sizeof(uint64_t));

    lm->blk_metas = (struct blk_meta*)calloc(meta->node->nchs * meta->node->nluns * meta->node->nblks, sizeof(struct blk_meta));
    lm->blk_entries = (struct blk_entry*)calloc(meta->node->nchs * meta->node->nluns * meta->node->nblks, sizeof(struct blk_entry));
    
    lm->blk_lists = (struct blk_list*)calloc(meta->node->nchs * meta->node->nluns, sizeof(struct blk_list));
    struct nodegeoaddr tgeoblk = {0, 0, 0, 0, 0};
    int chi, luni, blki;
    for (chi = 0; chi < meta->node->nchs; chi++) {
        tgeoblk.ch_i = chi;
        for (luni = 0; luni < meta->node->nluns; luni++) {
            tgeoblk.lun_i = luni;
            struct blk_list* listi = &(lm->blk_lists[chi + luni * meta->node->nchs]);
            TAILQ_INIT(&(listi->empty_blks));
            TAILQ_INIT(&(listi->non_empty_blks));
            for (blki = 0; blki < meta->node->nblks; blki++) {
                tgeoblk.blk_i = blki;
                uint64_t tblk = geoaddr2vblk(meta->node, &tgeoblk);
                struct blk_entry* tblk_entry = &(lm->blk_entries[tblk]);
                tblk_entry->pblk_i = tblk;
                tblk_entry->meta = &(lm->blk_metas[tblk]);
                TAILQ_INSERT_TAIL(&(listi->empty_blks), tblk_entry, pt);
            }
            listi->active_blk = NULL;
        }
    }

    int vpi;
    for (vpi = 0; vpi < meta->total_pagenum; vpi++) {
        // lm->vpg2ppg[pi] = pi;
        lm->vpg2ppg[vpi] = meta->total_pagenum;
    }
    int ppi;
    for (ppi = 0; ppi < meta->total_pagenum; ppi++) {
        lm->ppg2vpg[ppi] = meta->total_pagenum;
    }
    return 0;
}

static int free_ls_meta(struct ls_meta* lm) {
    free(lm->vpg2ppg);
    free(lm->ppg2vpg);
    free(lm->blk_metas);
    free(lm->blk_entries);
    free(lm->blk_lists);
    free(lm->blkbuf);
    free(lm->blkvpgs);
    return 0;
}

static uint64_t vpg2ppg(struct ls_meta* lm, uint64_t vpg_i) {
    return lm->vpg2ppg[vpg_i];
}

static uint64_t ppg2vpg(struct ls_meta* lm, uint64_t ppg_i) {
    return lm->ppg2vpg[ppg_i];
}

static struct nodegeoaddr vaddr2paddr(struct ls_meta* lm, struct nodegeoaddr* vaddr) {
    // convert page only, keep offset
    struct fox_node* node = lm->meta->node;
    uint64_t vpg_i = geoaddr2vpg(node, vaddr);
    uint64_t ppg_i = vpg2ppg(lm, vpg_i);
    struct nodegeoaddr paddr = vpg2geoaddr(node, ppg_i);
    paddr.offset_in_page = vaddr->offset_in_page;
    return paddr;
}

static int isalloc(struct ls_meta* lm, uint64_t vpg_i) {
    if (vpg2ppg(lm, vpg_i) == lm->meta->total_pagenum)
        return 0;
    else
        return 1;
}

static uint64_t allocate_page(struct ls_meta* lm, uint64_t vpg_i);

static int garbage_collection(struct ls_meta* lm, uint64_t vpg_i_begin, uint64_t vpg_i_end) {
    struct fox_node* node = lm->meta->node;
    if (lm->clean_pg_count == lm->meta->total_pagenum)
        return 0;
    struct timeval tvalst, tvaled;
    gettimeofday(&tvalst, NULL);
    // abandon pages to rewrite
    uint64_t vpg_i = 0;
    for (vpg_i = vpg_i_begin; vpg_i <= vpg_i_end; vpg_i++) {
        if (isalloc(lm, vpg_i)) {
            uint64_t oldppg = lm->vpg2ppg[vpg_i];
            lm->meta->page_state[oldppg] = PAGE_ABANDONED;
            lm->dirty_pg_count--;
            lm->abandoned_pg_count++;
            lm->vpg2ppg[vpg_i] = lm->meta->total_pagenum;
            lm->ppg2vpg[oldppg] = lm->meta->total_pagenum;
            uint64_t oldblk = vpg2vblk(node, oldppg);
            lm->blk_metas[oldblk].ndirtypgs--;
            lm->blk_metas[oldblk].nabandonedpgs++;
        }
    }
    // find the full block with min dirty pages, greedy
    uint64_t ited_ch_lun_num = 0;
    struct blk_entry* torecyc = NULL;
    struct blk_list* torecyc_list = NULL;
    uint64_t mindirtypgs = node->npgs;
    for (ited_ch_lun_num = 0; ited_ch_lun_num < node->nchs * node->nluns; ited_ch_lun_num++) {
        lm->next_ch_lun_i = (lm->next_ch_lun_i + ited_ch_lun_num) % (node->nchs * node->nluns);
        struct blk_list* listi = &(lm->blk_lists[lm->next_ch_lun_i]);
        if (!TAILQ_EMPTY(&(listi->non_empty_blks))) {
            struct blk_entry* tentry;
            TAILQ_FOREACH(tentry, &(listi->non_empty_blks), pt) {
                // printf("%d\n", tentry->meta->ndirtypgs);
                if (tentry->meta->ndirtypgs < mindirtypgs) {
                    mindirtypgs = tentry->meta->ndirtypgs;
                    torecyc = tentry;
                    torecyc_list = listi;
                }
            }
        }
    }
    if (torecyc != NULL) {
        struct nodegeoaddr torecyc_geo = vblk2geoaddr(node, torecyc->pblk_i);
        uint64_t read_dpi = 0;
        for (torecyc_geo.pg_i = 0; torecyc_geo.pg_i < node->npgs; torecyc_geo.pg_i++) {
            uint64_t ppgi = geoaddr2vpg(node, &torecyc_geo);
            if (lm->meta->page_state[ppgi] == PAGE_DIRTY) {
                rw_inside_page(node, lm->blockbuf, lm->blkbuf + read_dpi * lm->meta->vpg_sz, lm->meta, &torecyc_geo, lm->meta->vpg_sz, READ_MODE);
                lm->blkvpgs[read_dpi] = ppg2vpg(lm, ppgi);
                lm->ppg2vpg[ppgi] = lm->meta->total_pagenum;
                lm->vpg2ppg[lm->blkvpgs[read_dpi]] = lm->meta->total_pagenum;
                read_dpi++;
            }
        }
        uint64_t total_read = read_dpi;
        torecyc_geo.pg_i = 0;
        erase_block(node, lm->meta, &torecyc_geo);
        // printf("%d,%d,%d\n", torecyc->meta->nabandonedpgs, torecyc->meta->ndirtypgs, total_read);
        lm->dirty_pg_count -= torecyc->meta->ndirtypgs;
        lm->abandoned_pg_count -= torecyc->meta->nabandonedpgs;
        lm->clean_pg_count += node->npgs;
        torecyc->meta->ndirtypgs = 0;
        torecyc->meta->nabandonedpgs = 0;
        TAILQ_REMOVE(&(torecyc_list->non_empty_blks), torecyc, pt);
        TAILQ_INSERT_TAIL(&(torecyc_list->empty_blks), torecyc, pt);
        for (read_dpi = 0; read_dpi < total_read; read_dpi++) {
            uint64_t newppg = allocate_page(lm, lm->blkvpgs[read_dpi]);
            struct nodegeoaddr newppggeo = vpg2geoaddr(node, newppg);
            rw_inside_page(node, lm->blockbuf, lm->blkbuf + read_dpi * lm->meta->vpg_sz, lm->meta, &newppggeo, lm->meta->vpg_sz, WRITE_MODE);
        }
        lm->gc_map_change_count += total_read;
    }
    gettimeofday(&tvaled, NULL);
    lm->gc_count++;
    lm->gc_time += ((uint64_t)(tvaled.tv_sec - tvalst.tv_sec) * 1000000L + tvaled.tv_usec) - tvalst.tv_usec;
    return 0;
}

static uint64_t allocate_page(struct ls_meta* lm, uint64_t vpg_i) {
    struct fox_node* node = lm->meta->node;
    int allocedflag = isalloc(lm, vpg_i);
    if (allocedflag) {
        uint64_t oldppg = lm->vpg2ppg[vpg_i];
        lm->dirty_pg_count--;
        lm->abandoned_pg_count++;
        lm->meta->page_state[oldppg] = PAGE_ABANDONED;
        lm->ppg2vpg[oldppg] = lm->meta->total_pagenum;
        lm->vpg2ppg[vpg_i] = lm->meta->total_pagenum;
        uint64_t oldpblk = vpg2vblk(node, oldppg);
        lm->blk_metas[oldpblk].ndirtypgs--;
        lm->blk_metas[oldpblk].nabandonedpgs++;
    }
    // find first chlun with available empty blocks
    uint64_t ited_ch_lun_num = 0;
    struct blk_list* listi = NULL;
    for (ited_ch_lun_num = 0; ited_ch_lun_num <= node->nchs * node->nluns; ited_ch_lun_num++) {
        lm->next_ch_lun_i = (lm->next_ch_lun_i + ited_ch_lun_num) % (node->nchs * node->nluns);
        if (lm->blk_lists[lm->next_ch_lun_i].active_blk != NULL) {
            listi = &(lm->blk_lists[lm->next_ch_lun_i]);
            break;
        }
    }
    if (listi == NULL) {
        for (ited_ch_lun_num = 0; ited_ch_lun_num <= node->nchs * node->nluns; ited_ch_lun_num++) {
            lm->next_ch_lun_i = (lm->next_ch_lun_i + ited_ch_lun_num) % (node->nchs * node->nluns);
            if (!TAILQ_EMPTY(&(lm->blk_lists[lm->next_ch_lun_i].empty_blks))) {
                listi = &(lm->blk_lists[lm->next_ch_lun_i]);
                struct blk_entry* newemp = TAILQ_FIRST(&(listi->empty_blks));
                TAILQ_REMOVE(&(listi->empty_blks), newemp, pt);
                listi->active_blk = newemp;
                break;
            }
        }
    }
    if (listi == NULL) {
       //  printf("Impossible after GC!\n");
        return lm->meta->total_pagenum;
    } else {
        lm->next_ch_lun_i = (lm->next_ch_lun_i + 1) % (node->nchs * node->nluns);
        struct blk_entry* act = listi->active_blk;
        struct nodegeoaddr tgeo = vblk2geoaddr(node, act->pblk_i);
        tgeo.offset_in_page = 0;
        tgeo.pg_i = act->meta->ndirtypgs + act->meta->nabandonedpgs;
        uint64_t newppg = geoaddr2vpg(node, &tgeo);
        lm->vpg2ppg[vpg_i] = newppg;
        lm->ppg2vpg[newppg] = vpg_i;
        if (allocedflag)
            lm->map_change_count++;
        else
            lm->map_set_count++;
        lm->clean_pg_count--;
        lm->dirty_pg_count++;
        act->meta->ndirtypgs++;
        // remove if used up!
        if (act->meta->ndirtypgs + act->meta->nabandonedpgs == node->npgs) {
            listi->active_blk = NULL;
            TAILQ_INSERT_TAIL(&(listi->non_empty_blks), act, pt);
        }
        return newppg;
    }
}

static uint64_t alloc_gc(struct ls_meta* lm, uint64_t vpg_i, uint64_t vpg_i_begin, uint64_t vpg_i_end) {
    // uint64_t newppg = garbage_collection(lm, vpg_i_begin, vpg_i_end);
    uint64_t newppg = allocate_page(lm, vpg_i);
    while (newppg  == lm->meta->total_pagenum) {
        garbage_collection(lm, vpg_i_begin, vpg_i_end);
        newppg = allocate_page(lm, vpg_i);
    }
    return newppg;
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
        struct nodegeoaddr ppg_geo_end = vpg2geoaddr(node, vpg2ppg(lm, vpg_i_end));
        uint8_t* resbuf_t = resbuf;
        // read
        if (vpg_i_begin == vpg_i_end) {
            rw_inside_page(node, buf, resbuf_t, meta, &poffset_begin, size, mode);
            resbuf_t += size;
        } else {
            // read begin page
            rw_inside_page(node, buf, resbuf_t, meta, &poffset_begin, vpg_sz - poffset_begin.offset_in_page, mode);
            resbuf_t += (vpg_sz - poffset_begin.offset_in_page);
            // read middle pages
            if (vpg_i_end - vpg_i_begin > 1) {
                uint64_t middle_pgi;
                for (middle_pgi = vpg_i_begin + 1; middle_pgi < vpg_i_end; middle_pgi++) {
                    struct nodegeoaddr tgeo = vpg2geoaddr(node, vpg2ppg(lm, middle_pgi));
                    rw_inside_page(node, buf, resbuf_t, meta, &tgeo, vpg_sz, mode);
                    resbuf_t += vpg_sz;
                }
            }
            // read end page
            rw_inside_page(node, buf, resbuf_t, meta, &ppg_geo_end, poffset_end.offset_in_page + 1, mode);
            resbuf_t += (poffset_end.offset_in_page + 1);
        }
    } else if (mode == WRITE_MODE) {
        struct nodegeoaddr vpg_geo_begin = vpg2geoaddr(node, vpg_i_begin);
        struct nodegeoaddr vpg_geo_end = vpg2geoaddr(node, vpg_i_end);
        struct nodegeoaddr ppg_geo_begin = vaddr2paddr(lm, &vpg_geo_begin);
        struct nodegeoaddr ppg_geo_end = vaddr2paddr(lm, &vpg_geo_end);
        uint8_t* resbuf_t = resbuf;
        // read first and last pages (if necessary) before GC!
        if (isalloc(lm, vpg_i_begin) && (((vpg_i_begin == vpg_i_end) && (voffset_begin.offset_in_page != 0 || voffset_end.offset_in_page != vpg_sz - 1)) || ((vpg_i_begin < vpg_i_end) && (voffset_begin.offset_in_page != 0))))
            rw_inside_page(node, buf, meta->begin_pagebuf, meta, &ppg_geo_begin, vpg_sz, READ_MODE);
        if (isalloc(lm, vpg_i_end) && ((vpg_i_begin < vpg_i_end) && (voffset_end.offset_in_page != vpg_sz - 1)))
            rw_inside_page(node, buf, meta->end_pagebuf, meta, &ppg_geo_end, vpg_sz, READ_MODE);
        while (lm->clean_pg_count < vpg_i_end - vpg_i_begin + 1) {
            garbage_collection(lm, vpg_i_begin, vpg_i_end);
        }
        // read or write...
        if (vpg_i_begin == vpg_i_end) {
            if (isalloc(lm, vpg_i_begin) && (voffset_begin.offset_in_page != 0 || voffset_end.offset_in_page != vpg_sz - 1)) {
            }
            // alloc a new page here
            uint64_t newppg = alloc_gc(lm, vpg_i_begin, vpg_i_begin, vpg_i_end);
            struct nodegeoaddr newppgaddr = vpg2geoaddr(node, newppg);
            memcpy(meta->begin_pagebuf + voffset_begin.offset_in_page, resbuf_t, size);
            rw_inside_page(node, buf, meta->begin_pagebuf, meta, &newppgaddr, vpg_sz, mode);
            resbuf_t += size;
        } else {
            // rw begin page
            if (isalloc(lm, vpg_i_begin) && (voffset_begin.offset_in_page != 0)) {
            }
            uint64_t newppg = alloc_gc(lm, vpg_i_begin, vpg_i_begin, vpg_i_end);
            struct nodegeoaddr newppgaddr = vpg2geoaddr(node, newppg);
            memcpy(meta->begin_pagebuf + voffset_begin.offset_in_page, resbuf_t, vpg_sz - voffset_begin.offset_in_page);
            rw_inside_page(node, buf, meta->begin_pagebuf, meta, &newppgaddr, vpg_sz, mode);
            resbuf_t += (vpg_sz - voffset_begin.offset_in_page);
            // rw middle pages
            if (vpg_i_end - vpg_i_begin > 1) {
                uint64_t middle_pgi;
                for (middle_pgi = vpg_i_begin + 1; middle_pgi < vpg_i_end; middle_pgi++) {
                    newppg = alloc_gc(lm, middle_pgi, 1, 0);
                    newppgaddr = vpg2geoaddr(node, newppg);
                    rw_inside_page(node, buf, resbuf_t, meta, &newppgaddr, vpg_sz, mode);
                    resbuf_t += vpg_sz;
                }
            }
            // rw end page
            if (isalloc(lm, vpg_i_end) && (voffset_end.offset_in_page != vpg_sz - 1)) {
            }
            newppg = alloc_gc(lm, vpg_i_end, 1, 0);
            newppgaddr = vpg2geoaddr(node, newppg);
            memcpy(meta->end_pagebuf, resbuf_t, voffset_end.offset_in_page + 1);
            rw_inside_page(node, buf, meta->end_pagebuf, meta, &newppgaddr, vpg_sz, mode);
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

static struct fox_engine rewrite_ls_greedy_engine = {
    .id             = FOX_ENGINE_6,
    .name           = "rewrite_ls_greedy",
    .start          = rewrite_ls_start,
    .exit           = rewrite_ls_exit,
};

int foxeng_rewrite_ls_greedy_init (struct fox_workload *wl)
{
    return fox_engine_register(&rewrite_ls_greedy_engine);
}
