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

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;

public class ModernCockroachConnection
        extends Connection<ModernCockroachDatabase>
{
    protected ModernCockroachConnection(ModernCockroachDatabase database, java.sql.Connection connection)
    {
        super(database, connection);
    }

    @Override
    public ModernCockroachSchema getSchema(String name)
    {
        return new ModernCockroachSchema(jdbcTemplate, database, name);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath()
            throws SQLException
    {
        String schema = jdbcTemplate.getConnection().getSchema();
        if (schema == null) {
            if (!StringUtils.hasText((jdbcTemplate.getConnection().getCatalog()))) {
                throw new FlywayException("Unable to determine current database. Set the database in the JDBC URL.");
            }
            throw new FlywayException("Unable to determine current schema");
        }
        return schema;
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema)
            throws SQLException
    {
        jdbcTemplate.getConnection().setSchema(schema);
    }
}
