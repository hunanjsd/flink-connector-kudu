/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.colloh.flink.kudu.connector.internal.writer;

import lombok.SneakyThrows;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.*;
import org.colloh.flink.kudu.connector.internal.KuduTableInfo;
import org.colloh.flink.kudu.connector.internal.failure.DefaultKuduFailureHandler;
import org.colloh.flink.kudu.connector.internal.failure.KuduFailureHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * todo write kudu table config
 *
 * @param <T>
 */
@Internal
public class KuduWriter<T> implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final AbstractSingleOperationMapper<T> operationMapper;

    private final transient KuduClient client;
    private final transient KuduSession session;
    private final transient KuduTable table;

    private static final String MANUAL_FLUSH_BUFFER_FULL = "MANUAL_FLUSH is enabled but the buffer is too big";

    private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("kudu-flush-schedule-pool-%d").daemon(true).build());

    public KuduWriter(KuduTableInfo tableInfo, KuduWriterConfig writerConfig,
                      AbstractSingleOperationMapper<T> operationMapper) throws IOException {
        this(tableInfo, writerConfig, operationMapper, new DefaultKuduFailureHandler());
    }

    public KuduWriter(KuduTableInfo tableInfo, KuduWriterConfig writerConfig,
                      AbstractSingleOperationMapper<T> operationMapper,
                      KuduFailureHandler failureHandler) throws IOException {
        this.tableInfo = tableInfo;
        this.writerConfig = writerConfig;
        this.failureHandler = failureHandler;

        this.client = obtainClient();
        // 支持ignore operation kudu1.14支持
//        this.client.supportsIgnoreOperations();
        this.session = obtainSession();
        this.table = obtainTable();
        this.operationMapper = operationMapper;

        executorService.scheduleAtFixedRate(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                try {
                    session.flush();
                } catch (KuduException e) {
                    checkAsyncErrors();
                }
            }
        },10,1, TimeUnit.SECONDS);
    }

    private KuduClient obtainClient() {
        return new KuduClient.KuduClientBuilder(writerConfig.getMasters())
                .build();
    }

    private KuduSession obtainSession() {
        KuduSession session = client.newSession();
        session.setFlushMode(writerConfig.getFlushMode());
        return session;
    }

    private KuduTable obtainTable() throws IOException {
        String tableName = tableInfo.getName();
        if (client.tableExists(tableName)) {
            return client.openTable(tableName);
        }
        if (tableInfo.getCreateTableIfNotExists()) {
            return client.createTable(tableName, tableInfo.getSchema(), tableInfo.getCreateTableOptions());
        }
        throw new RuntimeException("Table " + tableName + " does not exist.");
    }

    public void write(T input) throws IOException {
        checkAsyncErrors();

        for (Operation operation : operationMapper.createOperations(input, table)) {
            // 针对MANUAL_FLUSH模式，要手工flush
            // MANUAL_FLUSH: the call returns when the operation has been added to the buffer,
            // else it throws a KuduException if the buffer is full.
            if (session.getFlushMode() == SessionConfiguration.FlushMode.MANUAL_FLUSH) {
                try {
                    session.apply(operation);
                } catch (KuduException e) {
                    if (MANUAL_FLUSH_BUFFER_FULL.equals(e.getMessage())) {
                        session.flush();
                        // buffer满了以后，下一条数据没有处理到，会丢数据
                        session.apply(operation);
                    }
                }
            } else {
                checkErrors(session.apply(operation));
            }
        }
    }

    public void flushAndCheckErrors() throws IOException {
        checkAsyncErrors();
        flush();
        checkAsyncErrors();
    }

    @VisibleForTesting
    public DeleteTableResponse deleteTable() throws IOException {
        String tableName = table.getName();
        return client.deleteTable(tableName);
    }

    @Override
    public void close() throws IOException {
        try {
            flushAndCheckErrors();
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception e) {
                log.error("Error while closing session.", e);
            }
            try {
                if (client != null) {
                    client.close();
                }
            } catch (Exception e) {
                log.error("Error while closing client.", e);
            }
        }
    }

    private void flush() throws IOException {
        session.flush();
    }

    private void checkErrors(OperationResponse response) throws IOException {
        if (response != null && response.hasRowError()) {
            failureHandler.onFailure(Arrays.asList(response.getRowError()));
        } else {
            checkAsyncErrors();
        }
    }

    private void checkAsyncErrors() throws IOException {
        if (session.countPendingErrors() == 0) {
            return;
        }

        List<RowError> errors = Arrays.asList(session.getPendingErrors().getRowErrors());
        failureHandler.onFailure(errors);
    }
}
