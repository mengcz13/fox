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
 * (ch,lun,blk,pg)
 * (0,0,0,0)
 * (0,0,0,1)
 * (0,0,0,2)
 * (0,0,1,0)
 * (0,0,1,1)
 * (0,0,1,2)
 * (0,1,0,0)
 * (0,1,0,1) ...
 */

#include <stdio.h>
#include <stdlib.h>
#include "../fox.h"
#include "fox-rewrite.h"

uint64_t geo2vpg(struct fox_node* node, int ch_i, int lun_i, int blk_i, int pg_i) {
    return (uint64_t)ch_i + (uint64_t)lun_i * node->nchs + (uint64_t)pg_i * node->nchs * node->nluns + (uint64_t)blk_i * node->nchs * node->nluns * node->npgs;
}

uint64_t geo2vblk(struct fox_node* node, int ch_i, int lun_i, int blk_i) {
    return (uint64_t)ch_i + (uint64_t)lun_i * node->nchs + (uint64_t)blk_i * node->nchs * node->nluns;
}

uint8_t* get_p_page_state(struct rewrite_meta* meta, int ch_i, int lun_i, int blk_i, int pg_i) {
    return &(meta->page_state[geo2vpg(meta->node, ch_i, lun_i, blk_i, pg_i)]);
}

uint8_t* get_p_blk_state(struct rewrite_meta* meta, int ch_i, int lun_i, int blk_i) {
    return &(meta->blk_state[geo2vblk(meta->node, ch_i, lun_i, blk_i)]);
}

int init_rewrite_meta(struct fox_node* node, struct rewrite_meta* meta) {
    meta->node = node;
    meta->page_state = (uint8_t*)calloc(node->nluns * node->nchs * node->nblks * node->npgs, sizeof(uint8_t));
    meta->blk_state = (uint8_t*)calloc(node->nluns * node->nchs * node->nblks, sizeof(uint8_t));
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
    free(meta->begin_pagebuf);
    free(meta->end_pagebuf);
    free(meta->pagebuf);
    return 0;
}

int set_byte_addr(struct fox_node* node, struct byte_addr* baddr, uint64_t lbyte_addr) {
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    uint64_t max_addr_in_node = (uint64_t)node->nchs * node->nluns * node->nblks * node->npgs * vpg_sz - 1;
    
    if (lbyte_addr > max_addr_in_node) {
        printf("Logic addr 0x%" PRIx64 " exceeds max addr in node 0x%" PRIx64 "!", lbyte_addr, max_addr_in_node);
        return 1;
    }

    uint64_t b_chs = node->nchs;
    uint64_t b_luns = node->nluns * b_chs;
    uint64_t b_pgs = node->npgs * b_luns;
    
    baddr->logic_byte_addr = lbyte_addr;
    baddr->offset_in_page = (uint32_t)(lbyte_addr % vpg_sz);
    uint64_t vpg_i = lbyte_addr / vpg_sz;
    baddr->vpg_i = vpg_i;

    baddr->ch_i = (uint32_t)(vpg_i % b_chs);
    baddr->lun_i = (uint32_t)((vpg_i / b_chs) % node->nluns);
    baddr->pg_i = (uint32_t)((vpg_i / b_luns) % node->npgs);
    baddr->blk_i = (uint32_t)((vpg_i / b_pgs) % node->nblks);

    baddr->offset_in_block = baddr->pg_i * vpg_sz + baddr->offset_in_page;

    baddr->vblk_i = baddr->ch_i + baddr->lun_i * b_chs + baddr->blk_i * b_luns;
    return 0;
}

int iterate_byte_addr(struct fox_node* node, struct fox_blkbuf* buf, struct rewrite_meta* meta, uint64_t offset, uint64_t size, int mode) {
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    // main func to handle each IO...
    // iterate based on channel->lun->page->block
    // maintain a state table for each page: clean/dirty/abandoned... (need considering a FSM!)
    // naive version: hit->erase->write
    // log-structured version: hit->abandon->alloc new
    struct byte_addr offset_begin;
    struct byte_addr offset_end;
    set_byte_addr(node, &offset_begin, offset);
    set_byte_addr(node, &offset_end, offset + size - 1); // [offset_begin, offset_end]

    if (mode == WRITE_MODE) {
        // erase if necessary...
        uint64_t vpg_i_begin = offset_begin.vpg_i;
        uint64_t vpg_i_end = offset_end.vpg_i;
        uint64_t vpgi;
        struct byte_addr vpgibyteaddr;
        rw_inside_page(node, buf, meta->begin_pagebuf, meta, offset_begin.ch_i, offset_begin.lun_i, offset_begin.blk_i, offset_begin.pg_i, 0, vpg_sz, READ_MODE);
        rw_inside_page(node, buf, meta->end_pagebuf, meta, offset_end.ch_i, offset_end.lun_i, offset_end.blk_i, offset_end.pg_i, 0, vpg_sz, READ_MODE);
        for (vpgi = vpg_i_begin; vpgi <= vpg_i_end; vpgi++) {
            set_byte_addr(node, &vpgibyteaddr, vpgi * vpg_sz);
            if (meta->blk_state[vpgibyteaddr.vblk_i] == BLOCK_DIRTY) {
                uint8_t covered_blk_state = BLOCK_CLEAN;
                int pgi_inblk;
                uint64_t cvpgi = vpgi;
                int begin_pgi_inblk = vpgibyteaddr.pg_i;
                int end_pgi_inblk;
                for (pgi_inblk = vpgibyteaddr.pg_i; pgi_inblk < node->npgs && cvpgi <= vpg_i_end; pgi_inblk++) {
                    if (meta->page_state[cvpgi] == PAGE_DIRTY) {
                        covered_blk_state = BLOCK_DIRTY;
                    }
                    cvpgi += (node->nchs * node->nluns);
                }
                end_pgi_inblk = pgi_inblk - 1;
                if (covered_blk_state == BLOCK_DIRTY) {
                    read_block(node, buf, vpgibyteaddr.ch_i, vpgibyteaddr.lun_i, vpgibyteaddr.blk_i);
                    erase_block(node, vpgibyteaddr.ch_i, vpgibyteaddr.lun_i, vpgibyteaddr.blk_i);
                    if (begin_pgi_inblk > 0) {
                        int wi_inblk;
                        for (wi_inblk = 0; wi_inblk < begin_pgi_inblk; wi_inblk++) {
                            rw_inside_page(node, buf, buf->buf_r + wi_inblk * vpg_sz, meta, vpgibyteaddr.ch_i, vpgibyteaddr.lun_i, vpgibyteaddr.blk_i, wi_inblk, 0, vpg_sz, WRITE_MODE);
                        }
                    }
                    if (end_pgi_inblk < node->npgs - 1) {
                        int wi_inblk;
                        for (wi_inblk = end_pgi_inblk + 1; wi_inblk < node->npgs; wi_inblk++) {
                            rw_inside_page(node, buf, buf->buf_r + wi_inblk * vpg_sz, meta, vpgibyteaddr.ch_i, vpgibyteaddr.lun_i, vpgibyteaddr.blk_i, wi_inblk, 0, vpg_sz, WRITE_MODE);
                        }
                    }
                }
                meta->blk_state[vpgibyteaddr.vblk_i] = BLOCK_CLEAN;
            }
        }
    }

    if (mode == READ_MODE || mode == WRITE_MODE) {
        // read or write...
        if (offset_begin.vpg_i == offset_end.vpg_i) {
            rw_inside_page(node, buf, meta->pagebuf, meta, offset_begin.ch_i, offset_begin.lun_i, offset_begin.blk_i, offset_begin.pg_i, offset_begin.offset_in_page, size, mode);
        } else {
            // rw begin page
            rw_inside_page(node, buf, meta->pagebuf, meta, offset_begin.ch_i, offset_begin.lun_i, offset_begin.blk_i, offset_begin.pg_i, offset_begin.offset_in_page, vpg_sz - offset_begin.offset_in_page, mode);
            // rw middle pages
            if (offset_end.vpg_i - offset_begin.vpg_i > 1) {
                uint64_t middle_pgn = offset_end.vpg_i - offset_begin.vpg_i - 2;
                uint64_t middle_pgi;
                struct byte_addr middle_pgi_begin;
                uint64_t pg_aligned_middle_addri = offset / vpg_sz * vpg_sz;
                pg_aligned_middle_addri += vpg_sz;
                for (middle_pgi = 0; middle_pgi < middle_pgn; middle_pgi++) {
                    set_byte_addr(node, &middle_pgi_begin, pg_aligned_middle_addri);
                    rw_inside_page(node, buf, meta->pagebuf, meta, middle_pgi_begin.ch_i, middle_pgi_begin.lun_i, middle_pgi_begin.blk_i, middle_pgi_begin.pg_i, 0, vpg_sz, mode);
                    pg_aligned_middle_addri += vpg_sz;
                }
            }
            // rw end page
            rw_inside_page(node, buf, meta->pagebuf, meta, offset_end.ch_i, offset_end.lun_i, offset_end.blk_i, offset_end.pg_i, 0, offset_end.offset_in_page + 1, mode);
        }
        return 0;
    }
    return 0;
}

int rw_inside_page(struct fox_node* node, struct fox_blkbuf* blockbuf, uint8_t* databuf, struct rewrite_meta* meta, int ch_i, int lun_i, int blk_i, int pg_i, uint32_t offset, uint32_t size, int mode) {
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
        if (offset == 0 && size == vpg_sz) {
            memcpy(blockbuf->buf_w + vpg_sz * pg_i, databuf, size);
            if (fox_write_blk(&node->vblk_tgt, node, blockbuf, 1, pg_i)) {
                return 1;
            }
        } else {
            if (fox_read_blk(&node->vblk_tgt, node, blockbuf, 1, pg_i)) {
                return 1;
            }
            memcpy(blockbuf->buf_w + vpg_sz * pg_i, blockbuf->buf_r + vpg_sz * pg_i, vpg_sz);
            memcpy(blockbuf->buf_w + vpg_sz * pg_i + offset, databuf, size);
            if (fox_write_blk(&node->vblk_tgt, node, blockbuf, 1, pg_i)) {
                return 1;
            }
        }
        uint8_t* pgst = get_p_page_state(meta, ch_i, lun_i, blk_i, pg_i);
        *pgst = PAGE_DIRTY;
        uint8_t* blkst = get_p_blk_state(meta, ch_i, lun_i, blk_i);
        *blkst = BLOCK_DIRTY;
    }
    return 0;
}

/*
int read_page(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i, int pg_i, int npgs) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_read_blk(&node->vblk_tgt, node, buf, npgs, pg_i);
}
*/

int read_block(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_read_blk(&node->vblk_tgt, node, buf, node->npgs, 0);
}

/*
int write_page(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i, int pg_i, int npgs) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_write_blk(&node->vblk_tgt, node, buf, npgs, pg_i);
}
*/

int write_block(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_write_blk(&node->vblk_tgt, node, buf, node->npgs, 0);
}

int erase_block(struct fox_node* node, int ch_i, int lun_i, int blk_i) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_erase_blk(&node->vblk_tgt, node);
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

    fox_start_node (node);

    do {
        // test total run time
        int erase_t;
        for (erase_t = 0; erase_t < 10; erase_t++) {
            fox_erase_all_vblks(node);
            printf("Erasing %d...", erase_t);
        }
        for (blk_i = 0; blk_i < t_blks; blk_i++) {
            ch_i = blk_i / blk_ch;
            lun_i = (blk_i % blk_ch) / blk_lun;

            fox_vblk_tgt(node, node->ch[ch_i],node->lun[lun_i],blk_i % blk_lun);

            if (node->wl->w_factor == 0)
                goto READ;

            pgoff_r = 0;
            pgoff_w = 0;
            while (pgoff_w < node->npgs) {
                if (node->wl->r_factor == 0)
                    npgs = node->npgs;
                else
                    npgs = (pgoff_w + node->wl->w_factor > node->npgs) ?
                                    node->npgs - pgoff_w : node->wl->w_factor;

                if (fox_write_blk(&node->vblk_tgt,node,&nbuf,npgs,pgoff_w))
                    goto BREAK;
                pgoff_w += npgs;

                aux_r = 0;
                while (aux_r < node->wl->r_factor) {
                    if (node->wl->w_factor == 0)
                        npgs = node->npgs;
                    npgs = (pgoff_r + node->wl->r_factor > pgoff_w) ?
                                    pgoff_w - pgoff_r : node->wl->r_factor;

                    if (fox_read_blk(&node->vblk_tgt,node,&nbuf,npgs,pgoff_r))
                        goto BREAK;

                    aux_r += npgs;
                    pgoff_r = (pgoff_r + node->wl->r_factor > pgoff_w) ?
                                                            0 : pgoff_r + npgs;
                }
            }

READ:
            /* 100 % reads */
            if (node->wl->w_factor == 0) {
                if (fox_read_blk (&node->vblk_tgt,node,&nbuf,node->npgs,0))
                    goto BREAK;
            }
            if (node->wl->r_factor > 0)
                fox_blkbuf_reset(node, &nbuf);
        }

BREAK:
        if ((node->wl->stats->flags & FOX_FLAG_DONE) || !node->wl->runtime ||
                                                   node->stats.progress >= 100)
            break;

        if (node->wl->w_factor != 0)
            if (fox_erase_all_vblks (node))
                break;

    } while (1);

    fox_end_node (node);
    fox_free_blkbuf (&nbuf, 1);
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
