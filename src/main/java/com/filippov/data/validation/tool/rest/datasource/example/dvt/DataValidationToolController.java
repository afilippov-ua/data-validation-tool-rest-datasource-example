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

import com.filippov.data.validation.tool.controller.AbstractDataValidationToolController;
import com.filippov.data.validation.tool.model.ColumnData;
import com.filippov.data.validation.tool.model.DatasourceColumn;
import com.filippov.data.validation.tool.model.DatasourceMetadata;
import com.filippov.data.validation.tool.model.DatasourceTable;
import com.filippov.data.validation.tool.rest.datasource.example.service.CompaniesService;
import com.filippov.data.validation.tool.rest.datasource.example.service.DataService;
import com.filippov.data.validation.tool.rest.datasource.example.service.DepartmentsService;
import com.filippov.data.validation.tool.rest.datasource.example.service.UsersService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Slf4j
@RestController
public class DataValidationToolController extends AbstractDataValidationToolController {

    private final StaticDataValidationToolMetadataProvider metadataProvider;
    private final Map<String, DataService<?>> serviceMap;

    public DataValidationToolController(StaticDataValidationToolMetadataProvider metadataProvider,
                                        UsersService usersService,
                                        DepartmentsService departmentsService,
                                        CompaniesService companiesService) {
        super();
        this.metadataProvider = metadataProvider;
        this.serviceMap = Map.of(
                StaticDataValidationToolMetadataProvider.USERS_TABLE.getName(), usersService,
                StaticDataValidationToolMetadataProvider.COMPANIES_TABLE.getName(), companiesService,
                StaticDataValidationToolMetadataProvider.DEPARTMENTS_TABLE.getName(), departmentsService);
    }

    @Override
    public DatasourceMetadata getMetadata() {
        return metadataProvider.getDatasourceMetadata();
    }

    @Override
    public ColumnData getData(DatasourceColumn datasourceColumn, int offset, int limit) {
        final DatasourceTable table = metadataProvider.getDatasourceTable(datasourceColumn.getTableName());
        final DatasourceColumn keyColumn = metadataProvider.getDatasourceColumn(table, table.getPrimaryKey());
        final DatasourceColumn valueColumn = metadataProvider.getDatasourceColumn(table, datasourceColumn.getName());

        final Function<Object, Object> keyExtractor = metadataProvider.getExtractor(keyColumn);
        final Function<Object, Object> valueExtractor = metadataProvider.getExtractor(valueColumn);

        final List<Object> keys = new ArrayList<>();
        final List<Object> values = new ArrayList<>();
        serviceMap.get(table.getName()).getData(offset, limit)
                .forEach(obj -> {
                    keys.add(keyExtractor.apply(obj));
                    values.add(valueExtractor.apply(obj));
                });

        return ColumnData.builder()
                .keyColumn(keyColumn)
                .dataColumn(valueColumn)
                .keys(keys)
                .values(values)
                .build();
    }

    @Override
    public int getSize(DatasourceColumn datasourceColumn) {
        final DatasourceTable table = metadataProvider.getDatasourceTable(datasourceColumn.getTableName());
        final DatasourceColumn column = metadataProvider.getDatasourceColumn(table, datasourceColumn.getName());
        return serviceMap.get(table.getName()).getSize();
    }
}
