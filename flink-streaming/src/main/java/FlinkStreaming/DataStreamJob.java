/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package FlinkStreaming;

import Deserializer.JSONValueDeserializationSchema;
import Dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {

    private static final String jdbcUrl = "jdbc:postgresql://postgres:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "eco2mix-national-tr";

        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers("broker:29092")
                .setTopics(topic)
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema()).build();

        DataStream<Transaction> consumptionDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");

        consumptionDataStream.print();

        // Create SQL table
        JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();


        //create transactions table
        consumptionDataStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS consumption (" +
                        "date_heure VARCHAR(255) PRIMARY KEY, " +
                        "consommation INT, " +
                        "prevision_j1 INT, " +
                        "prevision_j INT, " +
                        "fioul INT, " +
                        "charbon INT, " +
                        "gaz INT, " +
                        "nucleaire INT, " +
                        "eolien INT, " +
                        "eolien_terrestre VARCHAR(255), " +
                        "eolien_offshore VARCHAR(255), " +
                        "solaire INT, " +
                        "hydraulique INT, " +
                        "pompage INT, " +
                        "bioenergies INT, " +
                        "ech_physiques INT, " +
                        "taux_co2 INT, " +
                        "ech_comm_angleterre INT, " +
                        "ech_comm_espagne INT, " +
                        "ech_comm_italie INT, " +
                        "ech_comm_suisse VARCHAR(255), " +
                        "ech_comm_allemagne_belgique VARCHAR(255), " +
                        "fioul_tac INT, " +
                        "fioul_cogen INT, " +
                        "fioul_autres INT, " +
                        "gaz_tac INT, " +
                        "gaz_cogen INT, " +
                        "gaz_ccg INT, " +
                        "gaz_autres INT, " +
                        "hydraulique_fil_eau_eclusee INT, " +
                        "hydraulique_lacs INT, " +
                        "hydraulique_step_turbinage INT, " +
                        "bioenergies_dechets INT, " +
                        "bioenergies_biomasse INT, " +
                        "bioenergies_biogaz INT, " +
                        "stockage_batterie VARCHAR(255), " +
                        "destockage_batterie VARCHAR(255)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Consumption Table Sink");

        consumptionDataStream.addSink(JdbcSink.sink(
                "INSERT INTO consumption(" +
                        "date_heure, " +
                        "consommation, " +
                        "prevision_j1, " +
                        "prevision_j, " +
                        "fioul, " +
                        "charbon, " +
                        "gaz, " +
                        "nucleaire, " +
                        "eolien, " +
                        "eolien_terrestre, " +
                        "eolien_offshore, " +
                        "solaire, " +
                        "hydraulique, " +
                        "pompage, " +
                        "bioenergies, " +
                        "ech_physiques, " +
                        "taux_co2, " +
                        "ech_comm_angleterre, " +
                        "ech_comm_espagne, " +
                        "ech_comm_italie, " +
                        "ech_comm_suisse, " +
                        "ech_comm_allemagne_belgique, " +
                        "fioul_tac, " +
                        "fioul_cogen, " +
                        "fioul_autres, " +
                        "gaz_tac, " +
                        "gaz_cogen, " +
                        "gaz_ccg, " +
                        "gaz_autres, " +
                        "hydraulique_fil_eau_eclusee, " +
                        "hydraulique_lacs, " +
                        "hydraulique_step_turbinage, " +
                        "bioenergies_dechets, " +
                        "bioenergies_biomasse, " +
                        "bioenergies_biogaz, " +
                        "stockage_batterie, " +
                        "destockage_batterie" +
                        ") " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
                    preparedStatement.setString(1, transaction.getDate_heure());
                    preparedStatement.setInt(2, transaction.getConsommation());
                    preparedStatement.setInt(3, transaction.getPrevision_j1());
                    preparedStatement.setInt(4, transaction.getPrevision_j());
                    preparedStatement.setInt(5, transaction.getFioul());
                    preparedStatement.setInt(6, transaction.getCharbon());
                    preparedStatement.setInt(7, transaction.getGaz());
                    preparedStatement.setInt(8, transaction.getNucleaire());
                    preparedStatement.setInt(9, transaction.getEolien());
                    preparedStatement.setString(10, transaction.getEolien_terrestre());
                    preparedStatement.setString(11, transaction.getEolien_offshore());
                    preparedStatement.setInt(12, transaction.getSolaire());
                    preparedStatement.setInt(13, transaction.getHydraulique());
                    preparedStatement.setInt(14, transaction.getPompage());
                    preparedStatement.setInt(15, transaction.getBioenergies());
                    preparedStatement.setInt(16, transaction.getEch_physiques());
                    preparedStatement.setInt(17, transaction.getTaux_co2());
                    preparedStatement.setInt(18, transaction.getEch_comm_angleterre());
                    preparedStatement.setInt(19, transaction.getEch_comm_espagne());
                    preparedStatement.setInt(20, transaction.getEch_comm_italie());
                    preparedStatement.setString(21, transaction.getEch_comm_suisse());
                    preparedStatement.setString(22, transaction.getEch_comm_allemagne_belgique());
                    preparedStatement.setInt(23, transaction.getFioul_tac());
                    preparedStatement.setInt(24, transaction.getFioul_cogen());
                    preparedStatement.setInt(25, transaction.getFioul_autres());
                    preparedStatement.setInt(26, transaction.getGaz_tac());
                    preparedStatement.setInt(27, transaction.getGaz_cogen());
                    preparedStatement.setInt(28, transaction.getGaz_ccg());
                    preparedStatement.setInt(29, transaction.getGaz_autres());
                    preparedStatement.setInt(30, transaction.getHydraulique_fil_eau_eclusee());
                    preparedStatement.setInt(31, transaction.getHydraulique_lacs());
                    preparedStatement.setInt(32, transaction.getHydraulique_step_turbinage());
                    preparedStatement.setInt(33, transaction.getBioenergies_dechets());
                    preparedStatement.setInt(34, transaction.getBioenergies_biomasse());
                    preparedStatement.setInt(35, transaction.getBioenergies_biogaz());
                    preparedStatement.setString(36, transaction.getStockage_batterie());
                    preparedStatement.setString(37, transaction.getDestockage_batterie());
                },
                execOptions,
                connOptions
        )).name("Insert into consumption table sink");


        // Execute program, beginning computation.
        env.execute("Flink éCO2mix nationales temps réel");
    }
}