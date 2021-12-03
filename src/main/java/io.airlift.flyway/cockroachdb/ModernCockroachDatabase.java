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

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

public class ModernCockroachDatabase
        extends Database<ModernCockroachConnection>
{
    private static final Log LOG = LogFactory.getLog(ModernCockroachDatabase.class);

    public ModernCockroachDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor)
    {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
        LOG.info("CockroachDB version: " + getVersion());
    }

    @Override
    protected MigrationVersion determineVersion()
    {
        try {
            return MigrationVersion.fromVersion(new JdbcTemplate(rawMainJdbcConnection)
                    .queryForString("SELECT replace(value, 'v', '') FROM crdb_internal.node_build_info WHERE field = 'Version'"));
        }
        catch (SQLException e) {
            throw new FlywaySqlException("Unable to determine CockroachDB version", e);
        }
    }

    @Override
    protected ModernCockroachConnection doGetConnection(Connection connection)
    {
        return new ModernCockroachConnection(this, connection);
    }

    @Override
    public void ensureSupported()
    {
        ensureDatabaseIsRecentEnough("21.1");
    }

    @Override
    public boolean supportsDdlTransactions()
    {
        return true;
    }

    @Override
    public boolean supportsChangingCurrentSchema()
    {
        return true;
    }

    @Override
    public String getBooleanTrue()
    {
        return "TRUE";
    }

    @Override
    public String getBooleanFalse()
    {
        return "FALSE";
    }

    @Override
    public String doQuote(String identifier)
    {
        return getOpenQuote() + StringUtils.replaceAll(identifier, getCloseQuote(), getEscapedQuote()) + getCloseQuote();
    }

    @Override
    public String getEscapedQuote()
    {
        return "\"\"";
    }

    @Override
    public boolean catalogIsSchema()
    {
        return false;
    }

    @Override
    public boolean useSingleConnection()
    {
        return true;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline)
    {
        return "CREATE TABLE IF NOT EXISTS " + table + " (\n" +
                "    installed_rank int NOT NULL PRIMARY KEY,\n" +
                "    version string,\n" +
                "    description string NOT NULL,\n" +
                "    type string NOT NULL,\n" +
                "    script string NOT NULL,\n" +
                "    checksum int,\n" +
                "    installed_by string NOT NULL,\n" +
                "    installed_on timestamptz NOT NULL DEFAULT now(),\n" +
                "    execution_time int NOT NULL,\n" +
                "    success bool NOT NULL,\n" +
                "    INDEX (success)\n" +
                ");" +
                (baseline ? getBaselineStatement(table) + ";\n" : "");
    }
}
