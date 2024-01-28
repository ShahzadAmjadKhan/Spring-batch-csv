package io.github.lscsv.batch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import io.github.lscsv.batch.LscsvBatchService.LscsvInfo;

@Component
public class LscsvBatchApiClient {

    private final RestClient client;

    public LscsvBatchApiClient(@Value("${lscsv.rest.api.url:http://localhost:8080/api/v1/lscv/dummy}") String url) {
        this.client = RestClient.create(url);
    }

    public LscsvInfo processLscsv(LscsvInfo info) {
        return client.post()
                .body(info)
                .retrieve()
                .toEntity(LscsvInfo.class)
                .getBody();
    }
}
