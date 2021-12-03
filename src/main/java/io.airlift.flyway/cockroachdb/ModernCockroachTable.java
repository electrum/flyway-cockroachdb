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

import org.flywaydb.core.internal.command.DbMigrate;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

import static java.lang.Thread.currentThread;
import static java.util.Arrays.stream;

public class ModernCockroachTable
        extends Table<ModernCockroachDatabase, ModernCockroachSchema>
{
    public ModernCockroachTable(JdbcTemplate jdbcTemplate, ModernCockroachDatabase database, ModernCockroachSchema schema, String name)
    {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected boolean doExists()
            throws SQLException
    {
        // skip check when known to exist to avoid transaction retry errors
        if (stream(currentThread().getStackTrace()).anyMatch(element ->
                element.getClassName().equals(DbMigrate.class.getName()))) {
            return true;
        }

        return jdbcTemplate.queryForBoolean("" +
                "SELECT EXISTS (\n" +
                "  SELECT 1\n" +
                "  FROM [SHOW TABLES]\n" +
                "  WHERE type = 'table'\n" +
                "    AND schema_name = ?\n" +
                "    AND table_name = ?\n" +
                ")", schema.getName(), name);
    }

    @Override
    protected void doDrop()
            throws SQLException
    {
        throw new SQLException("Drop table not supported");
    }

    @Override
    public void lock() {}

    @Override
    protected void doLock() {}

    @Override
    public void unlock() {}
}
