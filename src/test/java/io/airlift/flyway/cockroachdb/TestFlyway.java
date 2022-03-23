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

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
public class TestFlyway
{
    @Container
    private final CockroachContainer cockroach = new CockroachContainer("cockroachdb/cockroach:latest-v21.2");

    @Test
    public void testSimpleMigration()
    {
        Flyway flyway = loadFlyway("/test-db-simple");
        Jdbi jdbi = createJdbi();

        assertThat(listTables(jdbi)).isEmpty();

        assertThat(flyway.migrate()).satisfies(result ->
                assertThat(result.migrationsExecuted).isEqualTo(1));

        assertThat(listTables(jdbi)).containsExactlyInAnyOrder("public.abc", "public.xyz", "public.flyway_schema_history");

        assertThat(flyway.migrate()).satisfies(result ->
                assertThat(result.migrationsExecuted).isEqualTo(0));

        assertThat(listTables(jdbi)).containsExactlyInAnyOrder("public.abc", "public.xyz", "public.flyway_schema_history");
    }

    @Test
    public void testTransactionalMigration()
    {
        Flyway flyway = loadFlyway("/test-db-transaction");
        Jdbi jdbi = createJdbi();

        assertThat(listTables(jdbi)).isEmpty();

        tryMigrate(flyway);
        assertThat(listTables(jdbi)).containsExactly("public.flyway_schema_history");

        tryMigrate(flyway);
        assertThat(listTables(jdbi)).containsExactly("public.flyway_schema_history");
    }

    private static void tryMigrate(Flyway flyway)
    {
        assertThatThrownBy(flyway::migrate)
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("Migration V1__Fail.sql failed")
                .hasMessageContaining("ERROR: type \"badtype\" does not exist");
    }

    private Flyway loadFlyway(String location)
    {
        return Flyway.configure()
                .dataSource(cockroach.getJdbcUrl(), cockroach.getUsername(), cockroach.getPassword())
                .locations(location)
                .failOnMissingLocations(true)
                .cleanDisabled(true)
                .load();
    }

    private Jdbi createJdbi()
    {
        return Jdbi.create(cockroach.getJdbcUrl(), cockroach.getUsername(), cockroach.getPassword());
    }

    private static List<String> listTables(Jdbi jdbi)
    {
        return jdbi.withHandle(handle ->
                handle.createQuery("SELECT schema_name || '.' || table_name FROM [SHOW TABLES]")
                        .mapTo(String.class)
                        .list());
    }
}
