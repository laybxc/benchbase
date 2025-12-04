/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.ycsb;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.TextGenerator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class YCSBLoader extends Loader<YCSBBenchmark> {
  private final int num_record;

  public YCSBLoader(YCSBBenchmark benchmark) {
    super(benchmark);
    this.num_record = (int) Math.round(YCSBConstants.RECORD_COUNT * this.scaleFactor);
    if (LOG.isDebugEnabled()) {
      LOG.debug("# of RECORDS:  {}", this.num_record);
    }
  }

  @Override
  public List<LoaderThread> createLoaderThreads() {
    List<LoaderThread> threads = new ArrayList<>();
    int count = 0;
    while (count < this.num_record) {
      final int start = count;
      final int stop = Math.min(start + YCSBConstants.THREAD_BATCH_SIZE, this.num_record);
      threads.add(
          new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
              if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("YCSBLoadThread[%d, %d]", start, stop));
              }
              loadRecords(conn, start, stop);
            }
          });
      count = stop;
    }
    return (threads);
  }

  private void loadRecords(Connection conn, int start, int stop) throws SQLException {
    // 获取两个表的元数据（假设它们结构相同）
    Table catalogTblR = benchmark.getCatalog().getTable("usertable_r");
    Table catalogTblW = benchmark.getCatalog().getTable("usertable_w");

    String sqlR = SQLUtil.getInsertSQL(catalogTblR, this.getDatabaseType());
    String sqlW = SQLUtil.getInsertSQL(catalogTblW, this.getDatabaseType());

    try (PreparedStatement stmtR = conn.prepareStatement(sqlR);
        PreparedStatement stmtW = conn.prepareStatement(sqlW)) {

      long total = 0;
      int batch = 0;

      for (int i = start; i < stop; i++) {
        // 为两个表设置相同的参数
        stmtR.setInt(1, i);
        stmtW.setInt(1, i);

        for (int j = 0; j < YCSBConstants.NUM_FIELDS; j++) {
          String fieldValue = TextGenerator.randomStr(rng(), benchmark.fieldSize);
          stmtR.setString(j + 2, fieldValue);
          stmtW.setString(j + 2, fieldValue);
        }

        stmtR.addBatch();
        stmtW.addBatch();

        total++;
        if (++batch >= workConf.getBatchSize()) {
          stmtR.executeBatch();
          stmtW.executeBatch();

          batch = 0;
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Records Loaded %d / %d", total, this.num_record));
          }
        }
      }

      // 处理剩余批次
      if (batch > 0) {
        stmtR.executeBatch();
        stmtW.executeBatch();
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Records Loaded %d / %d", total, this.num_record));
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished loading usertable_r and usertable_w");
    }
  }
}
