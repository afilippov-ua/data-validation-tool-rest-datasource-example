/*
 *   Copyright 2018-2020 the original author or authors.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.filippov.data.validation.tool.rest.datasource.example.dvt;

import com.filippov.data.validation.tool.model.DataType;
import com.filippov.data.validation.tool.model.DatasourceColumn;
import com.filippov.data.validation.tool.model.DatasourceMetadata;
import com.filippov.data.validation.tool.model.DatasourceTable;
import com.filippov.data.validation.tool.rest.datasource.example.model.Company;
import com.filippov.data.validation.tool.rest.datasource.example.model.Department;
import com.filippov.data.validation.tool.rest.datasource.example.model.User;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

@Component
public class StaticDataValidationToolMetadataProvider {

    public static final DatasourceTable DEPARTMENTS_TABLE = DatasourceTable.builder()
            .name("departments")
            .primaryKey("intId")
            .columns(asList("intId", "longId", "name", "numberOfEmployees", "employees"))
            .build();

    public static final DatasourceTable USERS_TABLE = DatasourceTable.builder()
            .name("users")
            .primaryKey("intId")
            .columns(asList("intId", "longId", "username", "password", "birthDate", "groupName"))
            .build();

    public static final DatasourceTable COMPANIES_TABLE = DatasourceTable.builder()
            .name("companies")
            .primaryKey("intId")
            .columns(asList("intId", "longId", "active", "companyName", "lastRevenue", "country", "dateOfCreation",
                    "foundersFirstNames", "foundersLastNames", "categories", "competitors"))
            .build();

    public static final DatasourceMetadata METADATA = DatasourceMetadata.builder()
            .tables(asList(DEPARTMENTS_TABLE, USERS_TABLE, COMPANIES_TABLE))
            .columns(asList(
                    DatasourceColumn.builder().tableName("departments").name("intId").dataType(DataType.INTEGER).build(),
                    DatasourceColumn.builder().tableName("departments").name("longId").dataType(DataType.LONG).build(),
                    DatasourceColumn.builder().tableName("departments").name("name").dataType(DataType.STRING).build(),
                    DatasourceColumn.builder().tableName("departments").name("numberOfEmployees").dataType(DataType.INTEGER).build(),
                    DatasourceColumn.builder().tableName("departments").name("employees").dataType(DataType.LIST_OF_OBJECTS).build(),
                    DatasourceColumn.builder().tableName("users").name("intId").dataType(DataType.INTEGER).build(),
                    DatasourceColumn.builder().tableName("users").name("longId").dataType(DataType.LONG).build(),
                    DatasourceColumn.builder().tableName("users").name("username").dataType(DataType.STRING).build(),
                    DatasourceColumn.builder().tableName("users").name("password").dataType(DataType.STRING).build(),
                    DatasourceColumn.builder().tableName("users").name("birthDate").dataType(DataType.DATE).build(),
                    DatasourceColumn.builder().tableName("users").name("groupName").dataType(DataType.STRING).build(),
                    DatasourceColumn.builder().tableName("companies").name("intId").dataType(DataType.INTEGER).build(),
                    DatasourceColumn.builder().tableName("companies").name("longId").dataType(DataType.LONG).build(),
                    DatasourceColumn.builder().tableName("companies").name("active").dataType(DataType.BOOLEAN).build(),
                    DatasourceColumn.builder().tableName("companies").name("companyName").dataType(DataType.STRING).build(),
                    DatasourceColumn.builder().tableName("companies").name("lastRevenue").dataType(DataType.DOUBLE).build(),
                    DatasourceColumn.builder().tableName("companies").name("country").dataType(DataType.STRING).build(),
                    DatasourceColumn.builder().tableName("companies").name("dateOfCreation").dataType(DataType.DATE_TIME).build(),
                    DatasourceColumn.builder().tableName("companies").name("foundersFirstNames").dataType(DataType.STRING).build(),
                    DatasourceColumn.builder().tableName("companies").name("foundersLastNames").dataType(DataType.STRING).build(),
                    DatasourceColumn.builder().tableName("companies").name("categories").dataType(DataType.LIST_OF_INTEGERS).build(),
                    DatasourceColumn.builder().tableName("companies").name("competitors").dataType(DataType.LIST_OF_STRINGS).build()))
            .build();

    private static final Map<String, DatasourceTable> TABLES = METADATA.getTables().stream()
            .collect(Collectors.toMap(
                    DatasourceTable::getName,
                    Function.identity()));

    private static final Map<DatasourceTable, Map<String, DatasourceColumn>> COLUMNS = METADATA.getTables().stream()
            .collect(Collectors.toMap(
                    Function.identity(),
                    table -> METADATA.getColumns().stream()
                            .filter(c -> c.getTableName().equals(table.getName()))
                            .collect(Collectors.toMap(
                                    DatasourceColumn::getName,
                                    Function.identity()))));

    private static final Map<String, Class<?>> DATA_TYPE_MAP = Map.of(
            USERS_TABLE.getName(), User.class,
            COMPANIES_TABLE.getName(), Company.class,
            DEPARTMENTS_TABLE.getName(), Department.class);

    public DatasourceMetadata getDatasourceMetadata() {
        return METADATA;
    }

    public Optional<DatasourceTable> getDatasourceTable(String tableName) {
        return Optional.ofNullable(TABLES.get(tableName));
    }

    public Optional<DatasourceColumn> getDatasourceColumn(DatasourceTable table, String columnName) {
        return Optional.ofNullable(COLUMNS.get(table))
                .map(m -> m.get(columnName));
    }

    public Class<?> getDataType(String tableName) {
        return DATA_TYPE_MAP.get(tableName);
    }
}
