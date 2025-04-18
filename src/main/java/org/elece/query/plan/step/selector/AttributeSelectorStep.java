package org.elece.query.plan.step.selector;

import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.utils.SerializationUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents the attribute selection step, where we select the desired columns from a row in their serialized format.
 */
public class AttributeSelectorStep extends SelectorStep {
    private final Table table;
    private final List<Column> selectedColumns;

    private final Integer newRowSize;

    public AttributeSelectorStep(Table table, List<Column> selectedColumns) {
        this.table = table;
        this.selectedColumns = selectedColumns;

        this.newRowSize = selectedColumns.stream().map(column -> column.getSqlType().getSize()).reduce(0, Integer::sum);
    }

    @Override
    public Optional<byte[]> next(DbObject dbObject) {
        if (Objects.isNull(newRowSize) || newRowSize <= 0) {
            return Optional.empty();
        }

        byte[] selectedData = new byte[newRowSize];
        for (Column column : selectedColumns) {
            byte[] valueOfField = SerializationUtils.getValueOfField(table, column, dbObject);
            SerializationUtils.setValueOfField(selectedColumns, column, valueOfField, selectedData);
        }


        return Optional.of(selectedData);
    }
}
