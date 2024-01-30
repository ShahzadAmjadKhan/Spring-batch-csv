package io.github.lscsv.batch;

import java.io.IOException;
import java.net.URI;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestClient;

import io.github.lscsv.batch.LscsvBatchService.LscsvInfo;

@Component
public class LscsvBatchApiClient {

    private final URI uri;
    private final RestClient client;

    public LscsvBatchApiClient(@Value("${lscsv.rest.api.url:http://localhost:8080/api/v1/lscv/dummy}") String url) {
        this.uri = URI.create(url);
        this.client = RestClient.create();
    }

    public LscsvInfo processLscsv(LscsvInfo info) {
        return client.post()
                .uri(uri)
                .body(info)
                .retrieve()
                .onStatus(new ResponseErrorHandler() {
                    
                    @Override
                    public boolean hasError(ClientHttpResponse response) throws IOException {
                        // TODO Auto-generated method stub
                        return false;
                    }
                    
                    @Override
                    public void handleError(ClientHttpResponse response) throws IOException {
                        // TODO Auto-generated method stub
                        
                    }
                })
                .toEntity(LscsvInfo.class)
                .getBody();
    }
}
