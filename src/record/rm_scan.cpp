/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    
    // 从第一个记录页开始
    rid_.page_no = RM_FIRST_RECORD_PAGE;
    rid_.slot_no = -1;
    
    // 找到第一个存放了记录的位置
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    
    // 获取文件头信息
    const RmFileHdr& file_hdr = file_handle_->file_hdr_;
    
    // 从当前位置的下一个slot开始查找
    while (rid_.page_no < file_hdr.num_pages) {
        // 获取当前页面的page handle
        RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);
        
        // 在当前页面中找下一个有记录的slot
        rid_.slot_no = Bitmap::next_bit(true, page_handle.bitmap, 
                                        file_hdr.num_records_per_page, rid_.slot_no);
        
        // 释放page handle
        file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        
        // 如果在当前页面找到了有记录的slot，返回
        if (rid_.slot_no < file_hdr.num_records_per_page) {
            return;
        }
        
        // 否则，移动到下一个页面
        rid_.page_no++;
        rid_.slot_no = -1;
    }
    
    // 到达文件末尾
    rid_.page_no = RM_NO_PAGE;
    rid_.slot_no = -1;
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    
    // 如果page_no为RM_NO_PAGE，表示已到达文件末尾
    return rid_.page_no == RM_NO_PAGE;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}