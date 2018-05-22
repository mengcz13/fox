#include "fox-rewrite-utils.h"

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
