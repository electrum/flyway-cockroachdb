/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.flyway.cockroachdb;

import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.DatabaseExecutionStrategy;
import org.flywaydb.core.internal.database.DefaultExecutionStrategy;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.cockroachdb.CockroachDBDatabaseType;
import org.flywaydb.core.internal.jdbc.ExecutionTemplate;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

import java.sql.Connection;

public class ModernCockroachDatabaseType
        extends CockroachDBDatabaseType
{
    @Override
    public String getName()
    {
        return "Modern CockroachDB";
    }

    @Override
    public int getPriority()
    {
        // make sure this overrides the parent
        return super.getPriority() + 1;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor)
    {
        return new ModernCockroachDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext)
    {
        return new ModernCockroachParser(configuration, parsingContext);
    }

    @Override
    public ExecutionTemplate createTransactionalExecutionTemplate(Connection connection, boolean rollbackOnException)
    {
        return new ModernCockroachExecutionTemplate(connection, rollbackOnException);
    }

    @Override
    public DatabaseExecutionStrategy createExecutionStrategy(Connection connection)
    {
        return new DefaultExecutionStrategy();
    }
}
