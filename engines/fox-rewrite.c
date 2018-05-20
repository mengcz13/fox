/*  - FOX - A tool for testing Open-Channel SSDs
 *      - Engine 1. Sequential I/O
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
#include "fox-rewrite.h"

uint64_t geoaddr2vpg(struct fox_node* node, struct nodegeoaddr* geoaddr) {
    uint64_t ch_i = geoaddr->ch_i;
    uint64_t lun_i = geoaddr->lun_i;
    uint64_t blk_i = geoaddr->blk_i;
    uint64_t pg_i = geoaddr->pg_i;
    return ch_i + lun_i * node->nchs + pg_i * node->nchs * node->nluns + blk_i * node->nchs * node->nluns * node->npgs;
}

uint64_t geoaddr2vblk(struct fox_node* node, struct nodegeoaddr* geoaddr) {
    uint64_t ch_i = geoaddr->ch_i;
    uint64_t lun_i = geoaddr->lun_i;
    uint64_t blk_i = geoaddr->blk_i;
    return ch_i + lun_i * node->nchs + blk_i * node->nchs * node->nluns;
}

struct nodegeoaddr vpg2geoaddr(struct fox_node* node, uint64_t vpg_i) {
    struct nodegeoaddr geoaddr;
    geoaddr.offset_in_page = 0; // aligned to page

    uint64_t b_chs = node->nchs;
    uint64_t b_luns = node->nluns * b_chs;
    uint64_t b_pgs = node->npgs * b_luns;

    geoaddr.ch_i = vpg_i % b_chs;
    geoaddr.lun_i = vpg_i / b_chs % node->nluns;
    geoaddr.pg_i = vpg_i / b_luns % node->npgs;
    geoaddr.blk_i = vpg_i / b_pgs % node->nblks;
    return geoaddr;
}

struct nodegeoaddr vblk2geoaddr(struct fox_node* node, uint64_t vblk_i) {
    struct nodegeoaddr geoaddr;
    geoaddr.offset_in_page = 0; // aligned to page
    geoaddr.pg_i = 0; //aligned to block

    uint64_t b_chs = node->nchs;
    uint64_t b_luns = node->nluns * b_chs;
    
    geoaddr.ch_i = vblk_i % b_chs;
    geoaddr.lun_i = vblk_i / b_chs % node->nluns;
    geoaddr.blk_i = vblk_i / b_luns % node->nblks;

    return geoaddr;
}

uint8_t* get_p_page_state(struct rewrite_meta* meta, struct nodegeoaddr* geoaddr) {
    return &(meta->page_state[geoaddr2vpg(meta->node, geoaddr)]);
}

uint8_t* get_p_blk_state(struct rewrite_meta* meta, struct nodegeoaddr* geoaddr) {
    return &(meta->blk_state[geoaddr2vblk(meta->node, geoaddr)]);
}

int init_rewrite_meta(struct fox_node* node, struct rewrite_meta* meta) {
    meta->node = node;
    meta->page_state = (uint8_t*)calloc(node->nluns * node->nchs * node->nblks * node->npgs, sizeof(uint8_t));
    meta->blk_state = (uint8_t*)calloc(node->nluns * node->nchs * node->nblks, sizeof(uint8_t));
    meta->temp_page_state_inblk = (uint8_t*)calloc(node->npgs, sizeof(uint8_t));
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    meta->begin_pagebuf = (uint8_t*)calloc(vpg_sz, sizeof(uint8_t));
    meta->end_pagebuf = (uint8_t*)calloc(vpg_sz, sizeof(uint8_t));
    meta->pagebuf = (uint8_t*)calloc(vpg_sz, sizeof(uint8_t));
    return 0;
}

int free_rewrite_meta(struct rewrite_meta* meta) {
    meta->node = NULL;
    free(meta->page_state);
    free(meta->blk_state);
    free(meta->temp_page_state_inblk);
    free(meta->begin_pagebuf);
    free(meta->end_pagebuf);
    free(meta->pagebuf);
    return 0;
}

int set_nodegeoaddr(struct fox_node* node, struct nodegeoaddr* baddr, uint64_t lbyte_addr) {
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    uint64_t max_addr_in_node = (uint64_t)node->nchs * node->nluns * node->nblks * node->npgs * vpg_sz - 1;
    if (lbyte_addr > max_addr_in_node) {
        printf("Logic addr 0x%" PRIx64 " exceeds max addr in node 0x%" PRIx64 "!", lbyte_addr, max_addr_in_node);
        return 1;
    }
    uint64_t vpg_i = lbyte_addr / vpg_sz;
    *baddr = vpg2geoaddr(node, vpg_i);
    baddr->offset_in_page = lbyte_addr % vpg_sz;
    return 0;
}

int iterate_io(struct fox_node* node, struct fox_blkbuf* buf, struct rewrite_meta* meta, uint8_t* resbuf, uint64_t offset, uint64_t size, int mode) {
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
        rw_inside_page(node, buf, meta->begin_pagebuf, meta, &vpgibegingeo, vpg_sz, READ_MODE);
        struct nodegeoaddr vpgiendgeo = vpg2geoaddr(node, vpg_i_end);
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
                memcpy(meta->begin_pagebuf + offset_begin.offset_in_page, resbuf_t, size);
                rw_inside_page(node, buf, meta->begin_pagebuf, meta, &vpg_geo_begin, vpg_sz, mode);
            }
            resbuf_t += size;
        } else {
            // rw begin page
            if (mode == READ_MODE) {
                rw_inside_page(node, buf, resbuf_t, meta, &offset_begin, vpg_sz - offset_begin.offset_in_page, mode);
            } else if (mode == WRITE_MODE) {
                memcpy(meta->begin_pagebuf + offset_begin.offset_in_page, resbuf_t, vpg_sz - offset_begin.offset_in_page);
                rw_inside_page(node, buf, meta->begin_pagebuf, meta, &vpg_geo_begin, vpg_sz, mode);
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
                memcpy(meta->end_pagebuf, resbuf_t, offset_end.offset_in_page + 1);
                rw_inside_page(node, buf, meta->end_pagebuf, meta, &vpg_geo_end, vpg_sz, mode);
            }
            resbuf_t += (offset_end.offset_in_page + 1);
        }
        return 0;
    }
    return 0;
}

int rw_inside_page(struct fox_node* node, struct fox_blkbuf* blockbuf, uint8_t* databuf, struct rewrite_meta* meta, struct nodegeoaddr* geoaddr, uint64_t size, int mode) {
    uint64_t ch_i = geoaddr->ch_i;
    uint64_t lun_i = geoaddr->lun_i;
    uint64_t blk_i = geoaddr->blk_i;
    uint64_t pg_i = geoaddr->pg_i;
    uint64_t offset = geoaddr->offset_in_page;
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    if (offset >= vpg_sz || offset + size > vpg_sz) {
        return 1;
    }
    if (mode == READ_MODE) {
        if (fox_read_blk(&node->vblk_tgt, node, blockbuf, 1, pg_i)) {
            return 1;
        }
        memcpy(databuf, blockbuf->buf_r + vpg_sz * pg_i + offset, size);
    } else if (mode == WRITE_MODE) {
        uint8_t* pgst = get_p_page_state(meta, geoaddr);
        if (*pgst == PAGE_DIRTY) {
            printf("Writing to dirty page!\n");
            printf("%" PRIu64 ", %" PRIu64 ", %" PRIu64 ", %" PRIu64 ", %" PRIu64 ", %d\n", ch_i, lun_i, blk_i, pg_i, offset, mode);
            return 1;
        } else if (offset == 0 && size == vpg_sz) {
            memcpy(blockbuf->buf_w + vpg_sz * pg_i, databuf, size);
            if (fox_write_blk(&node->vblk_tgt, node, blockbuf, 1, pg_i)) {
                return 1;
            }
        } else {
            /*
            if (fox_read_blk(&node->vblk_tgt, node, blockbuf, 1, pg_i)) {
                return 1;
            }
            memcpy(blockbuf->buf_w + vpg_sz * pg_i, blockbuf->buf_r + vpg_sz * pg_i, vpg_sz);
            memcpy(blockbuf->buf_w + vpg_sz * pg_i + offset, databuf, size);
            if (fox_write_blk(&node->vblk_tgt, node, blockbuf, 1, pg_i)) {
                return 1;
            }
            */
            // cannot rewrite before erasing!
            return 1;
        }
        *pgst = PAGE_DIRTY;
        uint8_t* blkst = get_p_blk_state(meta, geoaddr);
        *blkst = BLOCK_DIRTY;
    }
    return 0;
}

int read_block(struct fox_node* node, struct fox_blkbuf* buf, struct nodegeoaddr* geoaddr) {
    uint64_t ch_i = geoaddr->ch_i;
    uint64_t lun_i = geoaddr->lun_i;
    uint64_t blk_i = geoaddr->blk_i;
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_read_blk(&node->vblk_tgt, node, buf, node->npgs, 0);
}

int read_page(struct fox_node* node, struct fox_blkbuf* buf, struct nodegeoaddr* geoaddr) { 
    uint64_t ch_i = geoaddr->ch_i;
    uint64_t lun_i = geoaddr->lun_i;
    uint64_t blk_i = geoaddr->blk_i;
    uint64_t pg_i = geoaddr->pg_i;
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_read_blk(&node->vblk_tgt, node, buf, 1, pg_i);
}

int erase_block(struct fox_node* node, struct rewrite_meta* meta, struct nodegeoaddr* geoaddr) {
    uint64_t ch_i = geoaddr->ch_i;
    uint64_t lun_i = geoaddr->lun_i;
    uint64_t blk_i = geoaddr->blk_i;
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    if (fox_erase_blk(&node->vblk_tgt, node))
        return 1;
    *get_p_blk_state(meta, geoaddr) = BLOCK_CLEAN;
    struct nodegeoaddr tgeo = *geoaddr;
    for (tgeo.pg_i = 0; tgeo.pg_i < node->npgs; tgeo.pg_i++) {
        *get_p_page_state(meta, &tgeo) = PAGE_CLEAN;
    }
    return 0;
}

static int rewrite_start (struct fox_node *node)
{
    uint32_t t_blks;
    uint16_t t_luns, blk_lun, blk_ch, pgoff_r, pgoff_w, npgs, aux_r;
    int ch_i, lun_i, blk_i;
    node->stats.pgs_done = 0;
    struct fox_blkbuf nbuf;

    t_luns = node->nluns * node->nchs;
    t_blks = node->nblks * t_luns;
    blk_lun = t_blks / t_luns;
    blk_ch = blk_lun * node->nluns;

    if (fox_alloc_blk_buf (node, &nbuf))
        goto OUT;

    uint64_t iosize = 524288;
    uint8_t* databuf = calloc(iosize, sizeof(uint8_t));
    struct rewrite_meta meta;
    init_rewrite_meta(node, &meta);

    fox_start_node (node);

    int t;
    for (t = 0; t < 8; t++) {
        iterate_io(node, &nbuf, &meta, databuf, 0, iosize, WRITE_MODE);
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

static void rewrite_exit (void)
{
    return;
}

static struct fox_engine rewrite_engine = {
    .id             = FOX_ENGINE_4,
    .name           = "rewrite",
    .start          = rewrite_start,
    .exit           = rewrite_exit,
};

int foxeng_rewrite_init (struct fox_workload *wl)
{
    return fox_engine_register(&rewrite_engine);
}
