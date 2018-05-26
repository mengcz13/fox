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

/* ENGINE 5: Rewrite sequences like log-structured file systems:
 * Read as usual;
 * Write like log-structured file systems;
 * Erase when garbage collection.
 * Written by Chuizheng Meng <mengcz13@mails.tsinghua.edu.cn>
 */

#include <stdio.h>
#include <stdlib.h>
#include "../fox.h"
#include "fox-rewrite-utils.h"

struct ls_meta {
    struct rewrite_meta* meta;
    struct fox_blkbuf* blockbuf;
    uint64_t used_end_ppg;
    uint64_t used_begin_ppg;
    uint64_t dirty_pg_count;
    uint64_t abandoned_pg_count;
    uint64_t clean_pg_count;
    uint64_t* vpg2ppg;
    uint64_t* ppg2vpg;
    uint8_t* clblocks_buf;
    uint64_t* clblocks_vpgbuf;
};

static int init_ls_meta(struct rewrite_meta* meta, struct fox_blkbuf* blockbuf, struct ls_meta* lm) {
    lm->meta = meta;
    lm->blockbuf = blockbuf;
    lm->used_begin_ppg = 0;
    lm->used_end_ppg = 0;
    lm->dirty_pg_count = 0;
    lm->abandoned_pg_count = 0;
    lm->clean_pg_count = meta->total_pagenum;
    lm->vpg2ppg = (uint64_t*)calloc(meta->total_pagenum, sizeof(uint64_t));
    lm->ppg2vpg = (uint64_t*)calloc(meta->total_pagenum, sizeof(uint64_t));
    lm->clblocks_buf = (uint8_t*)calloc((uint64_t)meta->node->nluns * meta->node->nchs * 1 * meta->node->npgs * meta->vpg_sz, sizeof(uint8_t));
    lm->clblocks_vpgbuf = (uint64_t*)calloc((uint64_t)meta->node->nluns * meta->node->nchs * 1 * meta->node->npgs, sizeof(uint64_t));
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
    free(lm->clblocks_buf);
    free(lm->clblocks_vpgbuf);
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

static int garbage_collection(struct ls_meta* lm, uint64_t vpg_i_begin, uint64_t vpg_i_end) {
    if (lm->clean_pg_count == lm->meta->total_pagenum)
        return 0;
    // abandon pages to rewrite
    uint64_t vpg_i = 0;
    for (vpg_i = vpg_i_begin; vpg_i <= vpg_i_end; vpg_i++) {
        if (isalloc(lm, vpg_i)) {
            uint64_t oldppg = lm->vpg2ppg[vpg_i];
            lm->meta->page_state[oldppg] = PAGE_ABANDONED;
            lm->dirty_pg_count--;
            lm->abandoned_pg_count++;
        }
    }
    uint64_t used_last_ppg = (lm->used_end_ppg + lm->meta->total_pagenum - 1) % (lm->meta->total_pagenum);
    struct nodegeoaddr used_last_paddr = vpg2geoaddr(lm->meta->node, used_last_ppg);
    struct nodegeoaddr used_begin_paddr = vpg2geoaddr(lm->meta->node, lm->used_begin_ppg);
    uint64_t begin_clblock_i = used_begin_paddr.blk_i;
    uint64_t end_clblock_i = (used_last_paddr.blk_i + 1) % (lm->meta->node->nblks);
    struct nodegeoaddr nfblk_start;
    nfblk_start.offset_in_page = 0;
    nfblk_start.ch_i = 0;
    nfblk_start.lun_i = 0;
    nfblk_start.blk_i = end_clblock_i;
    nfblk_start.pg_i = 0;
    uint64_t nfblkppg_start = geoaddr2vpg(lm->meta->node, &nfblk_start);
    if (begin_clblock_i == end_clblock_i) {
        // have to use buffer!
        struct nodegeoaddr nfblk = nfblk_start;
        uint64_t nfblkppg = nfblkppg_start;
        uint64_t dirty_i = 0;
        uint64_t pgoff = 0;
        for (pgoff = 0; pgoff < lm->dirty_pg_count + lm->abandoned_pg_count + 1; pgoff++) {
            uint64_t currpg = (lm->used_begin_ppg + pgoff) % lm->meta->total_pagenum;
            struct nodegeoaddr currpgaddr = vpg2geoaddr(lm->meta->node, currpg);
            if (pgoff > 0 && (pgoff % (lm->meta->node->nchs * lm->meta->node->nluns * lm->meta->node->npgs) == 0 || pgoff == lm->dirty_pg_count + lm->abandoned_pg_count)) {
                uint64_t dirty_in_one = dirty_i;
                struct nodegeoaddr to_erase_block_addr = currpgaddr;
                if (pgoff % (lm->meta->node->nchs * lm->meta->node->nluns * lm->meta->node->npgs) == 0)
                    to_erase_block_addr.blk_i = (to_erase_block_addr.blk_i + lm->meta->node->nblks - 1) % lm->meta->node->nblks;
                to_erase_block_addr.offset_in_page = to_erase_block_addr.pg_i = 0;
                uint64_t ch_i = 0;
                uint64_t lun_i = 0;
                for (lun_i = 0; lun_i < lm->meta->node->nluns; lun_i++) {
                    for (ch_i = 0; ch_i < lm->meta->node->nchs; ch_i++) {
                        to_erase_block_addr.ch_i = ch_i;
                        to_erase_block_addr.lun_i = lun_i;
                        erase_block(lm->meta->node, lm->meta, &to_erase_block_addr);
                    }
                }
                // after cleaning, write from buffer to new clean area
                for (dirty_i = 0; dirty_i < dirty_in_one; dirty_i++) {
                    rw_inside_page(lm->meta->node, lm->blockbuf, lm->clblocks_buf + dirty_i * lm->meta->vpg_sz, lm->meta, &nfblk, lm->meta->vpg_sz, WRITE_MODE);
                    uint64_t vpgi = lm->clblocks_vpgbuf[dirty_i];
                    uint64_t oldppgi = lm->vpg2ppg[vpgi];
                    lm->vpg2ppg[vpgi] = nfblkppg;
                    lm->ppg2vpg[nfblkppg] = vpgi;
                    lm->ppg2vpg[oldppgi] = lm->meta->total_pagenum;
                    nfblkppg = (nfblkppg + 1) % lm->meta->total_pagenum;
                    nfblk = vpg2geoaddr(lm->meta->node, nfblkppg);
                }
                dirty_i = 0;
                if (pgoff == lm->dirty_pg_count + lm->abandoned_pg_count)
                    break;
            }
            if (lm->meta->page_state[currpg] == PAGE_DIRTY && pgoff < lm->dirty_pg_count + lm->abandoned_pg_count) {
                rw_inside_page(lm->meta->node, lm->blockbuf, lm->clblocks_buf + dirty_i * lm->meta->vpg_sz, lm->meta, &currpgaddr, lm->meta->vpg_sz, READ_MODE);
                lm->clblocks_vpgbuf[dirty_i] = lm->ppg2vpg[currpg];
                dirty_i++;
            }
        }
        lm->used_begin_ppg = geoaddr2vpg(lm->meta->node, &nfblk_start);
        lm->used_end_ppg = nfblkppg;
    } else {
        // we have at least 1 c * l * blocks!
        struct nodegeoaddr nfblk = nfblk_start;
        uint64_t nfblkppg = nfblkppg_start;
        uint64_t pgoff = 0;
        for (pgoff = 0; pgoff < lm->dirty_pg_count + lm->abandoned_pg_count + 1; pgoff++) {
            uint64_t currpg = (lm->used_begin_ppg + pgoff) % lm->meta->total_pagenum;
            struct nodegeoaddr currpgaddr = vpg2geoaddr(lm->meta->node, currpg);
            if (pgoff > 0 && (pgoff % (lm->meta->node->nchs * lm->meta->node->nluns * lm->meta->node->npgs) == 0 || pgoff == lm->dirty_pg_count + lm->abandoned_pg_count)) {
                struct nodegeoaddr to_erase_block_addr = currpgaddr;
                if (pgoff % (lm->meta->node->nchs * lm->meta->node->nluns * lm->meta->node->npgs) == 0)
                    to_erase_block_addr.blk_i = (to_erase_block_addr.blk_i + lm->meta->node->nblks - 1) % lm->meta->node->nblks;
                to_erase_block_addr.offset_in_page = to_erase_block_addr.pg_i = 0;
                uint64_t ch_i = 0;
                uint64_t lun_i = 0;
                for (lun_i = 0; lun_i < lm->meta->node->nluns; lun_i++) {
                    for (ch_i = 0; ch_i < lm->meta->node->nchs; ch_i++) {
                        to_erase_block_addr.ch_i = ch_i;
                        to_erase_block_addr.lun_i = lun_i;
                        erase_block(lm->meta->node, lm->meta, &to_erase_block_addr);
                    }
                }
                if (pgoff == lm->dirty_pg_count + lm->abandoned_pg_count)
                    break;
            }
            if (lm->meta->page_state[currpg] == PAGE_DIRTY) {
                rw_inside_page(lm->meta->node, lm->blockbuf, lm->meta->pagebuf, lm->meta, &currpgaddr, lm->meta->vpg_sz, READ_MODE);
                rw_inside_page(lm->meta->node, lm->blockbuf, lm->meta->pagebuf, lm->meta, &nfblk, lm->meta->vpg_sz, WRITE_MODE);
                uint64_t vpgi = lm->ppg2vpg[currpg];
                lm->vpg2ppg[vpgi] = nfblkppg;
                lm->ppg2vpg[nfblkppg] = vpgi;
                lm->ppg2vpg[currpg] = lm->meta->total_pagenum;
                nfblkppg = (nfblkppg + 1) % (lm->meta->total_pagenum);
                nfblk = vpg2geoaddr(lm->meta->node, nfblkppg);
            }
        }
        lm->used_begin_ppg = geoaddr2vpg(lm->meta->node, &nfblk_start);
        lm->used_end_ppg = nfblkppg;
    }
    lm->clean_pg_count += lm->abandoned_pg_count;
    lm->abandoned_pg_count = 0;
    return 0;
}

static uint64_t allocate_page(struct ls_meta* lm, uint64_t vpg_i) {
    if (isalloc(lm, vpg_i)) {
        uint64_t oldppg = lm->vpg2ppg[vpg_i];
        lm->meta->page_state[oldppg] = PAGE_ABANDONED;
        lm->dirty_pg_count--;
        lm->abandoned_pg_count++;
        lm->ppg2vpg[oldppg] = lm->meta->total_pagenum;
    }
    uint64_t newppg = lm->used_end_ppg;
    lm->used_end_ppg = (lm->used_end_ppg + 1) % lm->meta->total_pagenum;
    lm->vpg2ppg[vpg_i] = newppg;
    lm->ppg2vpg[newppg] = vpg_i;
    return newppg;
}

static int iterate_ls_io(struct fox_node* node, struct fox_blkbuf* buf, struct rewrite_meta* meta, struct ls_meta* lm, uint8_t* resbuf, uint64_t offset, uint64_t size, int mode) {
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    // main func to handle each IO...
    // iterate based on channel->lun->page->block
    // maintain a state table for each page: clean/dirty/abandoned... (need considering a FSM!)
    // log-structured version: hit->abandon->alloc new
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
        if (lm->clean_pg_count < vpg_i_end - vpg_i_begin + 1)
            garbage_collection(lm, vpg_i_begin, vpg_i_end);
        // read or write...
        if (vpg_i_begin == vpg_i_end) {
            if (isalloc(lm, vpg_i_begin) && (voffset_begin.offset_in_page != 0 || voffset_end.offset_in_page != vpg_sz - 1)) {
            }
            // alloc a new page here
            uint64_t newppg = allocate_page(lm, vpg_i_begin);
            struct nodegeoaddr newppgaddr = vpg2geoaddr(node, newppg);
            memcpy(meta->begin_pagebuf + voffset_begin.offset_in_page, resbuf_t, size);
            rw_inside_page(node, buf, meta->begin_pagebuf, meta, &newppgaddr, vpg_sz, mode);
            lm->clean_pg_count--;
            lm->dirty_pg_count++;
            resbuf_t += size;
        } else {
            // rw begin page
            if (isalloc(lm, vpg_i_begin) && (voffset_begin.offset_in_page != 0)) {
            }
            uint64_t newppg = allocate_page(lm, vpg_i_begin);
            struct nodegeoaddr newppgaddr = vpg2geoaddr(node, newppg);
            memcpy(meta->begin_pagebuf + voffset_begin.offset_in_page, resbuf_t, vpg_sz - voffset_begin.offset_in_page);
            rw_inside_page(node, buf, meta->begin_pagebuf, meta, &newppgaddr, vpg_sz, mode);
            lm->clean_pg_count--;
            lm->dirty_pg_count++;
            resbuf_t += (vpg_sz - voffset_begin.offset_in_page);
            // rw middle pages
            if (vpg_i_end - vpg_i_begin > 1) {
                uint64_t middle_pgi;
                for (middle_pgi = vpg_i_begin + 1; middle_pgi < vpg_i_end; middle_pgi++) {
                    newppg = allocate_page(lm, middle_pgi);
                    newppgaddr = vpg2geoaddr(node, newppg);
                    rw_inside_page(node, buf, resbuf_t, meta, &newppgaddr, vpg_sz, mode);
                    lm->clean_pg_count--;
                    lm->dirty_pg_count++;
                    resbuf_t += vpg_sz;
                }
            }
            // rw end page
            if (isalloc(lm, vpg_i_end) && (voffset_end.offset_in_page != vpg_sz - 1)) {
            }
            newppg = allocate_page(lm, vpg_i_end);
            newppgaddr = vpg2geoaddr(node, newppg);
            memcpy(meta->end_pagebuf, resbuf_t, voffset_end.offset_in_page + 1);
            rw_inside_page(node, buf, meta->end_pagebuf, meta, &newppgaddr, vpg_sz, mode);
            lm->clean_pg_count--;
            lm->dirty_pg_count++;
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

    uint64_t iosize = 524288;
    uint8_t* databuf = calloc(iosize, sizeof(uint8_t));
    struct rewrite_meta meta;
    init_rewrite_meta(node, &meta);
    struct ls_meta lm;
    init_ls_meta(&meta, &nbuf, &lm);

    fox_start_node (node);

    int t;
    for (t = 0; t < 8; t++) {
        iterate_ls_io(node, &nbuf, &meta, &lm, databuf, 0, iosize, WRITE_MODE);
    }
    /*
    do {
BREAK:
        if ((node->wl->stats->flags & FOX_FLAG_DONE) || !node->wl->runtime ||
                                                   node->stats.progress >= 100)
            break;

        if (node->wl->w_factor != 0)
            if (fox_erase_all_vblks (node))
                break;

    } while (1);
    */
    fox_end_node (node);

    fox_free_blkbuf (&nbuf, 1);
    free(databuf);
    free_rewrite_meta(&meta);
    free_ls_meta(&lm);
    return 0;

OUT:
    return -1;
}

static void rewrite_ls_exit (void)
{
    return;
}

static struct fox_engine rewrite_ls_engine = {
    .id             = FOX_ENGINE_5,
    .name           = "rewrite_ls",
    .start          = rewrite_ls_start,
    .exit           = rewrite_ls_exit,
};

int foxeng_rewrite_ls_init (struct fox_workload *wl)
{
    return fox_engine_register(&rewrite_ls_engine);
}
