/*  - FOX - A tool for testing Open-Channel SSDs
 *      - Engine 4. Rewrite inplace
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

/* ENGINE 4: Rewrite sequences:
 * Read/write in place, erase if necessary
 * Written by Chuizheng Meng <mengcz13@mails.tsinghua.edu.cn>
 */

#include <stdio.h>
#include <stdlib.h>
#include "../fox.h"
#include "fox-rewrite-utils.h"

static int iterate_inplace_io(struct fox_node* node, struct fox_blkbuf* buf, struct rewrite_meta* meta, uint8_t* resbuf, uint64_t offset, uint64_t size, int mode) {
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    // main func to handle each IO...
    // iterate based on channel->lun->page->block
    // maintain a state table for each page: clean/dirty/abandoned... (need considering a FSM!)
    // naive version: hit->erase->write
    // log-structured version: hit->abandon->alloc new
    struct nodegeoaddr offset_begin;
    struct nodegeoaddr offset_end;
    set_nodegeoaddr(node, &offset_begin, offset);
    set_nodegeoaddr(node, &offset_end, offset + size - 1); // [offset_begin, offset_end]
    uint64_t vpg_i_begin = offset / vpg_sz;
    uint64_t vpg_i_end = (offset + size - 1) / vpg_sz;

    if (mode == WRITE_MODE) {
        // erase if necessary...
        uint64_t vpgi;
        struct nodegeoaddr vpgibyteaddr;
        struct nodegeoaddr vpgibegingeo = vpg2geoaddr(node, vpg_i_begin);
        if (((vpg_i_begin == vpg_i_end) && (offset_begin.offset_in_page != 0 || offset_end.offset_in_page != vpg_sz - 1)) || ((vpg_i_begin < vpg_i_end) && (offset_begin.offset_in_page != 0)))
            rw_inside_page(node, buf, meta->begin_pagebuf, meta, &vpgibegingeo, vpg_sz, READ_MODE);
        struct nodegeoaddr vpgiendgeo = vpg2geoaddr(node, vpg_i_end);
        if ((vpg_i_begin < vpg_i_end) && (offset_end.offset_in_page != vpg_sz - 1))
            rw_inside_page(node, buf, meta->end_pagebuf, meta, &vpgiendgeo, vpg_sz, READ_MODE);
        for (vpgi = vpg_i_begin; vpgi <= vpg_i_end; vpgi++) {
            vpgibyteaddr = vpg2geoaddr(node, vpgi);
            if (*get_p_blk_state(meta, &vpgibyteaddr) == BLOCK_DIRTY) {
                uint8_t covered_blk_state = BLOCK_CLEAN;
                struct nodegeoaddr tgeo = vpgibyteaddr;
                int begin_pgi_inblk = vpgibyteaddr.pg_i;
                int end_pgi_inblk;
                // only erase when covered area has dirty pages
                // or we can write directly
                for (; tgeo.pg_i < node->npgs && geoaddr2vpg(node, &tgeo) <= vpg_i_end; tgeo.pg_i++) {
                    if (*get_p_page_state(meta, &tgeo) == PAGE_DIRTY) {
                        covered_blk_state = BLOCK_DIRTY;
                    }
                }
                end_pgi_inblk = tgeo.pg_i - 1;
                if (covered_blk_state == BLOCK_DIRTY) {
                    struct nodegeoaddr tgeo = vpgibyteaddr;
                    // collect page states in the block
                    for (tgeo.pg_i = 0; tgeo.pg_i < node->npgs; tgeo.pg_i++) {
                        meta->temp_page_state_inblk[tgeo.pg_i] = *get_p_page_state(meta, &tgeo);
                        if (tgeo.pg_i < begin_pgi_inblk || tgeo.pg_i > end_pgi_inblk) {
                            if (meta->temp_page_state_inblk[tgeo.pg_i] == PAGE_DIRTY) {
                                read_page(node, buf, &tgeo);
                            }
                        }
                    }
                    erase_block(node, meta, &vpgibyteaddr);
                    // rewrite former dirty pages outside covered area
                    for (tgeo.pg_i = 0; tgeo.pg_i < node->npgs; tgeo.pg_i++) {
                        assert(*get_p_page_state(meta, &tgeo) == PAGE_CLEAN);
                        if (tgeo.pg_i < begin_pgi_inblk || tgeo.pg_i > end_pgi_inblk) {
                            if (meta->temp_page_state_inblk[tgeo.pg_i] == PAGE_DIRTY) {
                                rw_inside_page(node, buf, buf->buf_r + tgeo.pg_i * vpg_sz, meta, &tgeo, vpg_sz, WRITE_MODE);
                            }
                        }
                    }
                }
                // mark checked blocks with BLOCK_CLEAN
                // this should be fine since these blocks will be written later and go to BLOCK_DIRTY again
                *get_p_blk_state(meta, &vpgibyteaddr) = BLOCK_CLEAN;
            }
        }
    }

    if (mode == READ_MODE || mode == WRITE_MODE) {
        struct nodegeoaddr vpg_geo_begin = vpg2geoaddr(node, vpg_i_begin);
        struct nodegeoaddr vpg_geo_end = vpg2geoaddr(node, vpg_i_end);
        uint8_t* resbuf_t = resbuf;
        // read or write...
        if (vpg_i_begin == vpg_i_end) {
            if (mode == READ_MODE) {
                rw_inside_page(node, buf, resbuf_t, meta, &offset_begin, size, mode);
            } else if (mode == WRITE_MODE) {
                if (offset_begin.offset_in_page != 0 || offset_end.offset_in_page != vpg_sz - 1) {
                    memcpy(meta->begin_pagebuf + offset_begin.offset_in_page, resbuf_t, size);
                    rw_inside_page(node, buf, meta->begin_pagebuf, meta, &vpg_geo_begin, vpg_sz, mode);
                } else {
                    rw_inside_page(node, buf, resbuf_t, meta, &offset_begin, size, mode);
                }
            }
            resbuf_t += size;
        } else {
            // rw begin page
            if (mode == READ_MODE) {
                rw_inside_page(node, buf, resbuf_t, meta, &offset_begin, vpg_sz - offset_begin.offset_in_page, mode);
            } else if (mode == WRITE_MODE) {
                if (offset_begin.offset_in_page != 0) {
                    memcpy(meta->begin_pagebuf + offset_begin.offset_in_page, resbuf_t, vpg_sz - offset_begin.offset_in_page);
                    rw_inside_page(node, buf, meta->begin_pagebuf, meta, &vpg_geo_begin, vpg_sz, mode);
                } else {
                    rw_inside_page(node, buf, resbuf_t, meta, &offset_begin, vpg_sz, mode);
                }
            }
            resbuf_t += (vpg_sz - offset_begin.offset_in_page);
            // rw middle pages
            if (vpg_i_end - vpg_i_begin > 1) {
                uint64_t middle_pgi;
                for (middle_pgi = vpg_i_begin + 1; middle_pgi < vpg_i_end; middle_pgi++) {
                    struct nodegeoaddr tgeo = vpg2geoaddr(node, middle_pgi);
                    rw_inside_page(node, buf, resbuf_t, meta, &tgeo, vpg_sz, mode);
                    resbuf_t += vpg_sz;
                }
            }
            // rw end page
            if (mode == READ_MODE) {
                rw_inside_page(node, buf, resbuf_t, meta, &vpg_geo_end, offset_end.offset_in_page + 1, mode);
            } else if (mode == WRITE_MODE) {
                if (offset_end.offset_in_page != vpg_sz - 1) {
                    memcpy(meta->end_pagebuf, resbuf_t, offset_end.offset_in_page + 1);
                    rw_inside_page(node, buf, meta->end_pagebuf, meta, &vpg_geo_end, vpg_sz, mode);
                } else {
                    rw_inside_page(node, buf, resbuf_t, meta, &vpg_geo_end, vpg_sz, mode);
                }
            }
            resbuf_t += (offset_end.offset_in_page + 1);
        }
        return 0;
    }
    return 0;
}

static int rewrite_inplace_start (struct fox_node *node)
{
    node->stats.pgs_done = 0;
    struct fox_blkbuf nbuf;

    if (fox_alloc_blk_buf (node, &nbuf))
        goto OUT;

    uint64_t iosize_single = 524288;
    uint64_t iosize = 4 * iosize_single;
    uint64_t offset_i = 0;
    uint8_t* databuf = (uint8_t*)calloc(iosize, sizeof(uint8_t));
    struct rewrite_meta meta;
    init_rewrite_meta(node, &meta);

    fox_start_node (node);

    int t;
    for (t = 0; t < 8; t++) {
        iterate_inplace_io(node, &nbuf, &meta, databuf, offset_i, iosize, WRITE_MODE);
        offset_i += iosize_single;
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
    return 0;

OUT:
    return -1;
}

static void rewrite_inplace_exit (void)
{
    return;
}

static struct fox_engine rewrite_inplace_engine = {
    .id             = FOX_ENGINE_4,
    .name           = "rewrite_inplace",
    .start          = rewrite_inplace_start,
    .exit           = rewrite_inplace_exit,
};

int foxeng_rewrite_inplace_init (struct fox_workload *wl)
{
    return fox_engine_register(&rewrite_inplace_engine);
}
