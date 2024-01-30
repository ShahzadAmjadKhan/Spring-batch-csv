package io.github.lscsv;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

@SpringBootTest
@AutoConfigureMockMvc
public class ApplicationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testGetIndexHtml() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/index.html"))
                .andExpect(MockMvcResultMatchers.status().is2xxSuccessful());
    }

    // @Test
    public void testLscsvBatch() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/api/v1/lscv")
                .param("file", "/tmp/custom_2017_2020.csv"))
                .andExpect(MockMvcResultMatchers.status().is2xxSuccessful())
                .andReturn();
    }
}
