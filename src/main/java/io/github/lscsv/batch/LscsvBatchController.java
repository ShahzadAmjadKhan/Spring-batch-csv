package io.github.lscsv.batch;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/lscv")
public class LscsvBatchController {

    private final LscsvBatchService service;

    public LscsvBatchController(LscsvBatchService service) {
        this.service = service;
    }

    @GetMapping("/{id}")
    public ResponseEntity<LscsvBatchService.JobInfo> findLscv(
            @PathVariable(name = "id") Long executionId) {
        LscsvBatchService.JobInfo info = service.findLscsvJob(executionId);
        if (info == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(info);
    }

    @GetMapping("/launch")
    public ResponseEntity<LscsvBatchService.JobInfo> launchLscv2(
            @RequestParam(name = "file") String file) {
        return ResponseEntity.ok(service.lanchLscsv(file));
    }

    @PostMapping
    public ResponseEntity<LscsvBatchService.JobInfo> launchLscv(
            @RequestParam(name = "file") String file) {
        return ResponseEntity.ok(service.lanchLscsv(file));
    }

    @PostMapping("/dummy")
    public ResponseEntity<LscsvBatchService.LscsvInfo> apiDummyLscv(@RequestBody LscsvBatchService.LscsvInfo info) {
        return ResponseEntity.ok(info);
    }
}
