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

struct nodegeoaddr {
    uint64_t offset_in_page; // parts not aligned to page (in ocssd)
    uint64_t ch_i;
    uint64_t lun_i;
    uint64_t blk_i;
    uint64_t pg_i;
};

struct rewrite_meta {
    struct fox_node* node;
    uint8_t* page_state;
    uint8_t* blk_state;
    uint8_t* begin_pagebuf;
    uint8_t* end_pagebuf;
    uint8_t* pagebuf;
};

uint64_t geoaddr2vpg(struct fox_node* node, struct nodegeoaddr* geoaddr);

uint64_t geoaddr2vblk(struct fox_node* node, struct nodegeoaddr* geoaddr);

struct nodegeoaddr vpg2geoaddr(struct fox_node* node, uint64_t vpg_i);

struct nodegeoaddr vblk2geoaddr(struct fox_node* node, uint64_t vblk_i);

uint8_t* get_p_page_state(struct rewrite_meta* meta, struct nodegeoaddr* geoaddr);

uint8_t* get_p_blk_state(struct rewrite_meta* meta, struct nodegeoaddr* geoaddr);

int init_rewrite_meta(struct fox_node* node, struct rewrite_meta* meta);

int free_rewrite_meta(struct rewrite_meta* meta);

int set_nodegeoaddr(struct fox_node* node, struct nodegeoaddr* baddr, uint64_t lbyte_addr);

int iterate_io(struct fox_node* node, struct fox_blkbuf* buf, struct rewrite_meta* meta, uint64_t offset, uint64_t size, int mode);

int rw_inside_page(struct fox_node* node, struct fox_blkbuf* blockbuf, uint8_t* databuf, struct rewrite_meta* meta, struct nodegeoaddr* geoaddr, uint64_t size, int mode);

/*
int read_page(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i, int pg_i, int npgs) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_read_blk(&node->vblk_tgt, node, buf, npgs, pg_i);
}
*/

int read_block(struct fox_node* node, struct fox_blkbuf* buf, struct nodegeoaddr* geoaddr);

/*
int write_page(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i, int pg_i, int npgs) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_write_blk(&node->vblk_tgt, node, buf, npgs, pg_i);
}
*/

int write_block(struct fox_node* node, struct fox_blkbuf* buf, struct nodegeoaddr* geoaddr);

int erase_block(struct fox_node* node, struct nodegeoaddr* geoaddr);

#endif
