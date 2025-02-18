/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.twilmes.sql.gremlin.adapter.converter;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlFactory;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select.GremlinSqlSelect;
import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import org.twilmes.sql.gremlin.adapter.results.SqlGremlinQueryResult;
import org.twilmes.sql.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;
import java.util.List;

/**
 * This module is the entry point of the SqlGremlin conversion.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class SqlConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlConverter.class);
    private static final List<RelTraitDef> TRAIT_DEFS =
            ImmutableList.of(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);
    private static final SqlParser.Config PARSER_CONFIG =
            SqlParser.config().withLex(Lex.MYSQL).withQuoting(Quoting.DOUBLE_QUOTE);
    private static final Program PROGRAM =
            Programs.sequence(Programs.ofRules(Programs.RULE_SET), Programs.CALC_PROGRAM);
    private final FrameworkConfig frameworkConfig;
    private final GremlinSchema gremlinSchema;

    public SqlConverter(final GremlinSchema gremlinSchema) {
        this.gremlinSchema = gremlinSchema;
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        this.frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(PARSER_CONFIG)
                .defaultSchema(rootSchema.add("gremlin", gremlinSchema))
                .traitDefs(TRAIT_DEFS)
                .programs(PROGRAM)
                .build();
    }

    private GremlinSqlSelect getSelect(final GraphTraversalSource g, final String query) throws SQLException {
        GremlinSqlFactory.setSqlMetadata(new SqlMetadata(gremlinSchema));
        final QueryPlanner queryPlanner = new QueryPlanner(frameworkConfig);
        queryPlanner.plan(query);
        final SqlNode sqlNode = queryPlanner.getValidate();
        if (sqlNode instanceof SqlSelect) {
            return GremlinSqlFactory.createSelect((SqlSelect) sqlNode, g);
        } else {
            throw SqlGremlinError.createNotSupported(SqlGremlinError.SQL_SELECT_ONLY);
        }
    }

    public SqlGremlinQueryResult executeQuery(final GraphTraversalSource g, final String query) throws SQLException {
        return getSelect(g, query).executeTraversal();
    }

    private GraphTraversal<?, ?> getGraphTraversal(GraphTraversalSource g, final String query) throws SQLException {
        return getSelect(g, query).generateTraversal();
    }

    public String getStringTraversal(final GraphTraversalSource g, final String query) throws SQLException {
        return GroovyTranslator.of("g").translate(getGraphTraversal(g, query).asAdmin().getBytecode());
    }

    @Getter
    private static class QueryPlanner {
        private final Planner planner;
        private SqlNode validate;

        QueryPlanner(final FrameworkConfig frameworkConfig) {
            this.planner = Frameworks.getPlanner(frameworkConfig);
        }

        public void plan(final String sql) throws SQLException {
            try {
                validate = planner.validate(planner.parse(sql));
            } catch (final Exception e) {
                throw SqlGremlinError.create(SqlGremlinError.PARSE_ERROR, e, sql);
            }
        }
    }
}
