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

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class ModernCockroachSchema
        extends Schema<ModernCockroachDatabase, ModernCockroachTable>
{
    public ModernCockroachSchema(JdbcTemplate jdbcTemplate, ModernCockroachDatabase database, String name)
    {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists()
            throws SQLException
    {
        return jdbcTemplate.queryForBoolean(
                "SELECT EXISTS (SELECT 1 FROM [SHOW SCHEMAS] WHERE schema_name = ?)", name);
    }

    @Override
    protected boolean doEmpty()
            throws SQLException
    {
        return jdbcTemplate.queryForBoolean("" +
                "SELECT NOT EXISTS (\n" +
                "  SELECT 1 FROM [SHOW TABLES] WHERE schema_name = ?\n" +
                "  UNION ALL\n" +
                "  SELECT 1 FROM [SHOW SEQUENCES] WHERE sequence_schema = ?\n" +
                "  UNION ALL\n" +
                "  SELECT 1 FROM [SHOW TYPES] WHERE schema = ? AND name <> 'crdb_internal_region'\n" +
                ")", name, name, name);
    }

    @Override
    protected void doCreate()
            throws SQLException
    {
        throw new SQLException("Create schema not supported");
    }

    @Override
    protected void doDrop()
            throws SQLException
    {
        throw new SQLException("Drop schema not supported");
    }

    @Override
    protected void doClean()
            throws SQLException
    {
        throw new SQLException("Clean not supported");
    }

    @Override
    protected ModernCockroachTable[] doAllTables()
            throws SQLException
    {
        throw new SQLException("Listing all tables not supported");
    }

    @Override
    public ModernCockroachTable getTable(String tableName)
    {
        return new ModernCockroachTable(jdbcTemplate, database, this, tableName);
    }
}
