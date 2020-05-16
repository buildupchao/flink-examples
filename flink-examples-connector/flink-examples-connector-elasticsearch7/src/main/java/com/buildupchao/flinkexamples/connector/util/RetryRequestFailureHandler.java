package com.buildupchao.flinkexamples.connector.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Optional;

/**
 * @author buildupchao
 * @date 2020/5/16 17:55
 * @since JDK1.8
 **/
@Slf4j
public class RetryRequestFailureHandler implements ActionRequestFailureHandler {

    @Override
    public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer) throws Throwable {
        if (ExceptionUtils.findThrowable(throwable, EsRejectedExecutionException.class).isPresent()) {
            requestIndexer.add(new ActionRequest[] { actionRequest });
        } else {
            if (ExceptionUtils.findThrowable(throwable, SocketTimeoutException.class).isPresent()) {
                return;
            } else {
                Optional<IOException> exp = ExceptionUtils.findThrowable(throwable, IOException.class);
                if (exp.isPresent()) {
                    IOException ioException = exp.get();
                    if (ioException != null && StringUtils.contains(ioException.getMessage(), "max retry timeout")) {
                        log.error(ioException.getMessage());
                    }
                }
            }
        }
        throw throwable;
    }
}
