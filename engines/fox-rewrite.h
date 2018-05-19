#ifndef FOX_REWRITE_H
#define FOX_REWRITE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

uint64_t geo2vpg(struct fox_node* node, int ch_i, int lun_i, int blk_i, int pg_i);

uint64_t geo2vblk(struct fox_node* node, int ch_i, int lun_i, int blk_i);

uint8_t* get_p_page_state(struct rewrite_meta* meta, int ch_i, int lun_i, int blk_i, int pg_i);

uint8_t* get_p_blk_state(struct rewrite_meta* meta, int ch_i, int lun_i, int blk_i);

int init_rewrite_meta(struct fox_node* node, struct rewrite_meta* meta);

int free_rewrite_meta(struct rewrite_meta* meta);

int set_byte_addr(struct fox_node* node, struct byte_addr* baddr, uint64_t lbyte_addr);

int iterate_byte_addr(struct fox_node* node, struct fox_blkbuf* buf, struct rewrite_meta* meta, uint64_t offset, uint64_t size, int mode);

int rw_inside_page(struct fox_node* node, struct fox_blkbuf* blockbuf, uint8_t* databuf, struct rewrite_meta* meta, int ch_i, int lun_i, int blk_i, int pg_i, uint32_t offset, uint32_t size, int mode);

/*
int read_page(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i, int pg_i, int npgs) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_read_blk(&node->vblk_tgt, node, buf, npgs, pg_i);
}
*/

int read_block(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i);

/*
int write_page(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i, int pg_i, int npgs) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_write_blk(&node->vblk_tgt, node, buf, npgs, pg_i);
}
*/

int write_block(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i);

int erase_block(struct fox_node* node, int ch_i, int lun_i, int blk_i);

#endif
