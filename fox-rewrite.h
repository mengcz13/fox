#ifndef FOX-REWRITE_H
#define FOX-REWRITE_H

#include <stdio.h>
#include <stdlib.h>
#include <liblightnvm.h>
#include "../fox.h"

struct byte_addr {
    uint32_t logic_byte_addr; // logic address aligned to byte (directly from logs)
    uint32_t offset_in_page; // parts not aligned to page (in ocssd)
    // uint32_t size; // when size > 0, byte_addr refers to range [byte_addr, byte_addr + size)
    int ch_i;
    int lun_i;
    int blk_i;
    int pg_i;
};

int set_byte_addr(struct fox_node* node, struct byte_addr* baddr, uint32_t lbyte_addr) {
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    uint32_t max_addr_in_node = node->nchs * node->nluns * node->nblks * node->npgs * vpg_sz - 1;
    
    if (lbyte_addr > max_addr_in_node) {
        printf("Logic addr 0x%x exceeds max addr in node 0x%x!", lbyte_addr, max_addr_in_node);
        return 1;
    }

    uint32_t b_chs = node->nchs;
    uint32_t b_luns = node->nluns * b_chs;
    uint32_t b_pgs = node->npgs * b_luns;
    
    baddr->logic_byte_addr = lbyte_addr;
    baddr->offset_in_page = lbyte_addr % vpg_sz;
    uint32_t vpg_i = lbyte_addr / vpg_sz;

    baddr->ch_i = vpg_i % b_chs;
    baddr->lun_i = vpg_i / b_chs % node->nluns;
    baddr->pg_i = vpg_i / b_luns % node->npgs;
    baddr->blk_i = vpg_i / b_pgs % node->nblks;

    baddr->size = 0;
    return 0;
}

int iterate_byte_addr(struct fox_node* node, struct fox_blkbuf* buf, struct byte_addr* baddr_begin, uint32_t size, int *(func)(struct fox_node*, struct fox_blkbuf*, int, int, int, int)) {
    // main func to handle each IO...
    // iterate based on channel->lun->page->block
    // maintain a state table for each page: clean/dirty/abandoned... (need considering a FSM!)
    // naive version: hit->erase->write
    // log-structured version: hit->abandon->alloc new
    return 0;
}

int read_page(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i, int pg_i) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_read_blk(&node->vblk_tgt, node, buf, 1, pg_i);
}

int write_page(struct fox_node* node, struct fox_blkbuf* buf, int ch_i, int lun_i, int blk_i, int pg_i) {
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    return fox_write_blk(&node->vblk_tgt, node, buf, 1, pg_i);
}

#endif
