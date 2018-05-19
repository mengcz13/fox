#ifndef FOX-REWRITE_H
#define FOX-REWRITE_H

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <liblightnvm.h>
#include "../fox.h"

#define READ_MODE 1
#define WRITE_MODE 2

struct byte_addr {
    uint64_t logic_byte_addr; // logic address aligned to byte (directly from logs)
    uint32_t offset_in_page; // parts not aligned to page (in ocssd)
    uint32_t offset_in_block; // parts not aligned to block (in ocssd)
    int ch_i;
    int lun_i;
    int blk_i;
    int pg_i;
};

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

    baddr->ch_i = (uint32_t)(vpg_i % b_chs);
    baddr->lun_i = (uint32_t)((vpg_i / b_chs) % node->nluns);
    baddr->pg_i = (uint32_t)((vpg_i / b_luns) % node->npgs);
    baddr->blk_i = (uint32_t)((vpg_i / b_pgs) % node->nblks);

    baddr->size = 0;
    baddr->offset_in_block = baddr->pg_i * vpg_sz + baddr->offset_in_page;
    return 0;
}

int iterate_byte_addr(struct fox_node* node, struct fox_blkbuf* buf, uint32_t offset, uint32_t size, int mode) {
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

    // erase blocks lun->channel
    int vluni;
    int vluni_begin = offset_begin->ch_i + offset_begin->lun_i * node->nchs;
    int vluni_end = offset_end->ch_i + offset_end->lun_i * node->nchs;
    for (vluni = 0; vluni < node->nluns * node->nchs; vluni++) {
        int chi = vluni % node->nchs;
        int luni = vluni / node->nchs;
        int blk_begin;
        int blk_end;
        uint32_t offset_in_block_begin;
        uint32_t offset_in_block_end;
        if (vluni < vluni_begin) {
            // next page, pageoff=0
        } else if (vluni == vluni_begin) {

        }
    }
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
