#ifndef FOX-REWRITE_H
#define FOX-REWRITE_H

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <liblightnvm.h>
#include "../fox.h"

#define READ_MODE 1
#define WRITE_MODE 2

#define PAGE_CLEAN 0
#define PAGE_DIRTY 1
#define PAGE_ABANDONED 2

#define BLOCK_CLEAN 0
#define BLOCK_DIRTY 1

struct byte_addr {
    uint64_t logic_byte_addr; // logic address aligned to byte (directly from logs)
    uint32_t offset_in_page; // parts not aligned to page (in ocssd)
    uint32_t offset_in_block; // parts not aligned to block (in ocssd)
    uint64_t vpg_i;
    uint32_t vblk_i;
    int ch_i;
    int lun_i;
    int blk_i;
    int pg_i;
};

struct rewrite_meta {
    struct fox_node* node;
    uint8_t* page_state;
    uint8_t* blk_state;
    uint8_t* begin_pagebuf;
    uint8_t* end_pagebuf;
    uint8_t* pagebuf;
};

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

    baddr->size = 0;
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
        uint64_t vpg_i_begin = offset_begin->vpg_i;
        uint64_t vpg_i_end = offset_end->vpg_i;
        uint64_t vpgi;
        uint64_t vpgiaddr;
        struct byte_addr vpgibyteaddr;
        rw_inside_page(node, buf, meta->begin_pagebuf, offset_begin->ch_i, offset_begin->lun_i, offset_begin->blk_i, offset_begin->pg_i, 0, vpg_sz, READ_MODE);
        rw_inside_page(node, buf, meta->end_pagebuf, offset_end->ch_i, offset_end->lun_i, offset_end->blk_i, offset_end->pg_i, 0, vpg_sz, READ_MODE);
        for (vpgi = vpg_i_begin; vpgi <= vpg_i_end; vpgi++) {
            set_byte_addr(node, &vpgibyteaddr, vpgi * vpg_sz);
            if (meta->blk_state[vpgibyteaddr->vblk_i] == BLOCK_DIRTY) {
                uint8_t covered_blk_state = BLOCK_CLEAN;
                int pgi_inblk;
                uint64_t cvpgi = vpgi;
                int begin_pgi_inblk = vpgibyteaddr->pgi;
                int end_pgi_inblk;
                for (pgi_inblk = vpgibyteaddr->pgi; pgi_inblk < node->npgs && cvpgi <= vpg_i_end; pgi_inblk++) {
                    if (meta->page_state[cvpgi] == PAGE_DIRTY) {
                        covered_blk_state = BLOCK_DIRTY;
                    }
                    cvpgi += (node->nchs * node->nluns);
                }
                end_pgi_inblk = pgi_inblk - 1;
                if (covered_blk_state == BLOCK_DIRTY) {
                    read_block(node, buf, vpgibyteaddr->ch_i, vpgibyteaddr->lun_i, vpgibyteaddr->blk_i);
                    erase_block(node, vpgibyteaddr->ch_i, vpgibyteaddr->lun_i, vpgibyteaddr->blk_i);
                    if (begin_pgi_inblk > 0) {
                        int wi_inblk;
                        for (wi_inblk = 0; wi_inblk < begin_pgi_inblk; wi_inblk++) {
                            rw_inside_page(node, buf, buf->buf_r + wi_inblk * vpg_sz, vpgibyteaddr->ch_i, vpgibyteaddr->lun_i, vpgibyteaddr->blk_i, wi_inblk, 0, vpg_sz, WRITE_MODE);
                        }
                    }
                    if (end_pgi_inblk < node->npgs - 1) {
                        int wi_inblk;
                        for (wi_inblk = end_pgi_inblk + 1; wi_inblk < node->npgs; wi_inblk++) {
                            rw_inside_page(node, buf, buf->buf_r + wi_inblk * vpg_sz, vpgibyteaddr->ch_i, vpgibyteaddr->lun_i, vpgibyteaddr->blk_i, wi_inblk, 0, vpg_sz, WRITE_MODE);
                        }
                    }
                }
                meta->blk_state[vpgibyteaddr->vblk_i] = BLOCK_CLEAN;
            }
        }
    }

    if (mode == READ_MODE || mode == WRITE_MODE) {
        // read or write...
        if (offset_begin->vpg_i == offset_end->vpg_i) {
            rw_inside_page(node, buf, meta->pagebuf, offset_begin->ch_i, offset_begin->lun_i, offset_begin->blk_i, offset_begin->pg_i, offset_begin->offset_in_page, size, mode);
        } else {
            // rw begin page
            rw_inside_page(node, buf, meta->pagebuf, offset_begin->ch_i, offset_begin->lun_i, offset_begin->blk_i, offset_begin->pg_i, offset_begin->offset_in_page, vpg_sz - offset_begin->offset_in_page, mode);
            // rw middle pages
            if (offset_end->vpg_i - offset_begin->vpg_i > 1) {
                uint64_t middle_pgn = offset_end->vpg_i - offset_begin->vpg_i - 2;
                uint64_t middle_pgi;
                struct byte_addr middle_pgi_begin;
                uint64_t pg_aligned_middle_addri = offset / vpg_sz * vpg_sz;
                pg_aligned_middle_addri += vpg_sz;
                for (middle_pgi = 0; middle_pgi < middle_pgn; middle_pgi++) {
                    set_byte_addr(node, &middle_pgi_begin, pg_aligned_middle_addri);
                    rw_inside_page(node, buf, meta->pagebuf, middle_pgi_begin->ch_i, middle_pgi_begin->lun_i, middle_pgi_begin->blk_i, middle_pgi_begin->pg_i, 0, vpg_sz, mode);
                    pg_aligned_middle_addri += vpg_sz;
                }
            }
            // rw end page
            rw_inside_page(node, buf, meta->pagebuf, offset_end->ch_i, offset_end->lun_i, offset_end->blk_i, offset_end->pg_i, 0, offset_end->offset_in_page + 1, mode);
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

#endif
