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
    meta->vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    meta->total_pagenum = node->nluns * node->nchs * node->nblks * node->npgs;
    meta->page_state = (uint8_t*)calloc(node->nluns * node->nchs * node->nblks * node->npgs, sizeof(uint8_t));
    meta->blk_state = (uint8_t*)calloc(node->nluns * node->nchs * node->nblks, sizeof(uint8_t));
    meta->temp_page_state_inblk = (uint8_t*)calloc(node->npgs, sizeof(uint8_t));
    size_t vpg_sz = meta->vpg_sz;
    meta->begin_pagebuf = (uint8_t*)calloc(vpg_sz, sizeof(uint8_t));
    meta->end_pagebuf = (uint8_t*)calloc(vpg_sz, sizeof(uint8_t));
    meta->pagebuf = (uint8_t*)calloc(vpg_sz, sizeof(uint8_t));
    meta->heatmap = (struct fox_heatmap_unit*)calloc(meta->total_pagenum, sizeof(struct fox_heatmap_unit));

    // read io sequence from file
    FILE* pfile = fopen(node->wl->inputiopath, "r");
    if (pfile == NULL)
        return 1;
    uint64_t t_offset, t_size, t_recnum;
    char t_iotype;
    if (fscanf(pfile, "%" PRId64, &t_recnum) == EOF)
        return 1;
    meta->ioseq = (struct fox_iounit*)calloc(t_recnum, sizeof(struct fox_iounit));
    meta->ioseqlen = t_recnum;
    uint64_t t_recnum_i;
    for (t_recnum_i = 0; t_recnum_i < t_recnum; t_recnum_i++) {
        fscanf(pfile, "%" PRId64 ",%" PRId64 ",%c", &t_offset, &t_size, &t_iotype);
        meta->ioseq[t_recnum_i].iotype = t_iotype;
        meta->ioseq[t_recnum_i].offset = t_offset;
        meta->ioseq[t_recnum_i].size = t_size;
        meta->ioseq[t_recnum_i].exetime = 0;
        meta->ioseq[t_recnum_i].gc_becost = 0;
        meta->ioseq[t_recnum_i].nabandoned = 0;
        meta->ioseq[t_recnum_i].ndirty = 0;
        meta->ioseq[t_recnum_i].nblock = 0;
    }
    fclose(pfile);

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
    free(meta->ioseq);
    free(meta->heatmap);
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
    uint64_t vpgofgeoaddr = geoaddr2vpg(node, geoaddr);
    fox_vblk_tgt(node, node->ch[ch_i], node->lun[lun_i], blk_i);
    size_t vpg_sz = node->wl->geo->page_nbytes * node->wl->geo->nplanes;
    if (offset >= vpg_sz || offset + size > vpg_sz) {
        return 1;
    }
    if (mode == READ_MODE) {
        if (fox_read_blk(&node->vblk_tgt, node, blockbuf, 1, pg_i)) {
            return 1;
        }
        meta->heatmap[vpgofgeoaddr].readt++;
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
            meta->heatmap[vpgofgeoaddr].writet++;
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
        uint64_t vpgoftgeo = geoaddr2vpg(node, &tgeo);
        meta->heatmap[vpgoftgeo].eraset++;
    }
    return 0;
}

int write_meta_stats(struct rewrite_meta* meta) {
    FILE *fp;
    char filename[40];

    // write heatmap
    sprintf(filename, "heatmap_fox_io.csv");
    fp = fopen(filename, "w");
    uint64_t vpgi = 0;
    for (vpgi = 0; vpgi < meta->total_pagenum; vpgi++) {
        struct nodegeoaddr tgeo = vpg2geoaddr(meta->node, vpgi);
        int blk = tgeo.blk_i;
        int pg = tgeo.pg_i;
        int ch = meta->node->ch[tgeo.ch_i];
        int lu = meta->node->lun[tgeo.lun_i];
        struct fox_heatmap_unit* hu = &meta->heatmap[vpgi];
        fprintf(fp, "%d,%d,%d,%d,%" PRId64 ",%" PRId64 ",%" PRId64 "\n", ch, lu, blk, pg, hu->readt, hu->writet, hu->eraset);
    }
    fclose(fp);

    // write io time
    sprintf(filename, "iotime_fox_io.csv");
    fp = fopen(filename, "w");
    uint64_t io_i = 0;
    for (io_i = 0; io_i < meta->ioseqlen; io_i++) {
        struct fox_iounit* ioseqi = &meta->ioseq[io_i];
        fprintf(fp, "%" PRId64 ",%" PRId64 ",%c,%" PRId64 ",%d,%d,%d,%lf\n", ioseqi->offset, ioseqi->size, ioseqi->iotype, ioseqi->exetime, ioseqi->nabandoned, ioseqi->ndirty, ioseqi->nblock, ioseqi->gc_becost);
    }
    fclose(fp);

    return 0;
}
