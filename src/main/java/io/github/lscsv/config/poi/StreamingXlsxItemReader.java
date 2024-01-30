package io.github.lscsv.config.poi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.io.Resource;

import com.monitorjbl.xlsx.StreamingReader;

public class StreamingXlsxItemReader<T> extends AbstractItemStreamItemReader<T> {

    private Resource resource;
    private Workbook workbook;
    // private Iterator<Sheet> sheets;
    private Iterator<Row> rows;
    private RowMapper<T> rowMapper;
    private int linesToSkip;

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public void setRowMapper(RowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
    }

    public void setLinesToSkip(int linesToSkip) {
        this.linesToSkip = linesToSkip;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        doOpen();
    }

    protected void doOpen() {
        try {
            workbook = StreamingReader.builder()
                    .bufferSize(8192)
                    .rowCacheSize(100)
                    .open(resource.getInputStream());
            // sheets = workbook.sheetIterator();
            // rows = sheets.next().rowIterator();
            rows = workbook.sheetIterator().next().rowIterator();
            for (int i = 0; i < linesToSkip && rows.hasNext(); i++) {
                rows.next();
            }
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public T read() {
        return doRead();
    }

    protected T doRead() {
        if (rows.hasNext()) {
            return rowMapper.mapRow(rows.next());
        }
        // while (sheets.hasNext()) {
        // rows = sheets.next().rowIterator();
        // if (rows.hasNext()) {
        // return rowMapper.mapRow(rows.next());
        // }
        // }
        return null;
    }

    @Override
    public void close() {
        doClose();
    }

    protected void doClose() {
        try {
            workbook.close();
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static interface RowMapper<T> {
        T mapRow(Row row);
    }
}