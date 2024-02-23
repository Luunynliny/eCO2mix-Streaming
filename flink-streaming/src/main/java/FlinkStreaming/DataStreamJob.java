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
import Dto.Consumption;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import static utils.JsonUtils.convertConsumptionToJson;

public class DataStreamJob {

    private static final String jdbcUrl = "jdbc:postgresql://postgres:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "eco2mix-national-tr";

        KafkaSource<Consumption> source = KafkaSource.<Consumption>builder()
                .setBootstrapServers("broker:29092")
                .setTopics(topic)
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema()).build();

        DataStream<Consumption> consumptionDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");

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

        consumptionDataStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS consumptions (" +
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
                (JdbcStatementBuilder<Consumption>) (preparedStatement, consumption) -> {

                },
                execOptions,
                connOptions
        )).name("Create Consumption Table Sink");

        // Insert data
        consumptionDataStream.addSink(JdbcSink.sink(
                "INSERT INTO consumptions(" +
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
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (date_heure) DO UPDATE SET " +
                        "consommation = EXCLUDED.consommation, " +
                        "prevision_j1 = EXCLUDED.prevision_j1, " +
                        "prevision_j = EXCLUDED.prevision_j, " +
                        "fioul = EXCLUDED.fioul, " +
                        "charbon = EXCLUDED.charbon, " +
                        "gaz = EXCLUDED.gaz, " +
                        "nucleaire = EXCLUDED.nucleaire, " +
                        "eolien = EXCLUDED.eolien, " +
                        "eolien_terrestre = EXCLUDED.eolien_terrestre, " +
                        "eolien_offshore = EXCLUDED.eolien_offshore, " +
                        "solaire = EXCLUDED.solaire, " +
                        "hydraulique = EXCLUDED.hydraulique, " +
                        "pompage = EXCLUDED.pompage, " +
                        "bioenergies = EXCLUDED.bioenergies, " +
                        "ech_physiques = EXCLUDED.ech_physiques, " +
                        "taux_co2 = EXCLUDED.taux_co2, " +
                        "ech_comm_angleterre = EXCLUDED.ech_comm_angleterre, " +
                        "ech_comm_espagne = EXCLUDED.ech_comm_espagne, " +
                        "ech_comm_italie = EXCLUDED.ech_comm_italie, " +
                        "ech_comm_suisse = EXCLUDED.ech_comm_suisse, " +
                        "ech_comm_allemagne_belgique = EXCLUDED.ech_comm_allemagne_belgique, " +
                        "fioul_tac = EXCLUDED.fioul_tac, " +
                        "fioul_cogen = EXCLUDED.fioul_cogen, " +
                        "fioul_autres = EXCLUDED.fioul_autres, " +
                        "gaz_tac = EXCLUDED.gaz_tac, " +
                        "gaz_cogen = EXCLUDED.gaz_cogen, " +
                        "gaz_ccg = EXCLUDED.gaz_ccg, " +
                        "gaz_autres = EXCLUDED.gaz_autres, " +
                        "hydraulique_fil_eau_eclusee = EXCLUDED.hydraulique_fil_eau_eclusee, " +
                        "hydraulique_lacs = EXCLUDED.hydraulique_lacs, " +
                        "hydraulique_step_turbinage = EXCLUDED.hydraulique_step_turbinage, " +
                        "bioenergies_dechets = EXCLUDED.bioenergies_dechets, " +
                        "bioenergies_biomasse = EXCLUDED.bioenergies_biomasse, " +
                        "bioenergies_biogaz = EXCLUDED.bioenergies_biogaz, " +
                        "stockage_batterie = EXCLUDED.stockage_batterie, " +
                        "destockage_batterie = EXCLUDED.destockage_batterie " +
                        "WHERE consumptions.date_heure = EXCLUDED.date_heure",
                (JdbcStatementBuilder<Consumption>) (preparedStatement, consumption) -> {
                    preparedStatement.setString(1, consumption.getDate_heure());
                    preparedStatement.setInt(2, consumption.getConsommation());
                    preparedStatement.setInt(3, consumption.getPrevision_j1());
                    preparedStatement.setInt(4, consumption.getPrevision_j());
                    preparedStatement.setInt(5, consumption.getFioul());
                    preparedStatement.setInt(6, consumption.getCharbon());
                    preparedStatement.setInt(7, consumption.getGaz());
                    preparedStatement.setInt(8, consumption.getNucleaire());
                    preparedStatement.setInt(9, consumption.getEolien());
                    preparedStatement.setString(10, consumption.getEolien_terrestre());
                    preparedStatement.setString(11, consumption.getEolien_offshore());
                    preparedStatement.setInt(12, consumption.getSolaire());
                    preparedStatement.setInt(13, consumption.getHydraulique());
                    preparedStatement.setInt(14, consumption.getPompage());
                    preparedStatement.setInt(15, consumption.getBioenergies());
                    preparedStatement.setInt(16, consumption.getEch_physiques());
                    preparedStatement.setInt(17, consumption.getTaux_co2());
                    preparedStatement.setInt(18, consumption.getEch_comm_angleterre());
                    preparedStatement.setInt(19, consumption.getEch_comm_espagne());
                    preparedStatement.setInt(20, consumption.getEch_comm_italie());
                    preparedStatement.setString(21, consumption.getEch_comm_suisse());
                    preparedStatement.setString(22, consumption.getEch_comm_allemagne_belgique());
                    preparedStatement.setInt(23, consumption.getFioul_tac());
                    preparedStatement.setInt(24, consumption.getFioul_cogen());
                    preparedStatement.setInt(25, consumption.getFioul_autres());
                    preparedStatement.setInt(26, consumption.getGaz_tac());
                    preparedStatement.setInt(27, consumption.getGaz_cogen());
                    preparedStatement.setInt(28, consumption.getGaz_ccg());
                    preparedStatement.setInt(29, consumption.getGaz_autres());
                    preparedStatement.setInt(30, consumption.getHydraulique_fil_eau_eclusee());
                    preparedStatement.setInt(31, consumption.getHydraulique_lacs());
                    preparedStatement.setInt(32, consumption.getHydraulique_step_turbinage());
                    preparedStatement.setInt(33, consumption.getBioenergies_dechets());
                    preparedStatement.setInt(34, consumption.getBioenergies_biomasse());
                    preparedStatement.setInt(35, consumption.getBioenergies_biogaz());
                    preparedStatement.setString(36, consumption.getStockage_batterie());
                    preparedStatement.setString(37, consumption.getDestockage_batterie());
                },
                execOptions,
                connOptions
        )).name("Insert into consumption table sink");

        // Send data to Elasticsearch
        consumptionDataStream.sinkTo(
                new Elasticsearch7SinkBuilder<Consumption>()
                        .setBulkFlushMaxActions(1)
                        .setHosts(new HttpHost("elasticsearch", 9200, "http"))
                        .setEmitter((consumption, runtimeContext, requestIndexer) -> {

                            String json = convertConsumptionToJson(consumption);

                            IndexRequest indexRequest = Requests.indexRequest()
                                    .index("consumption")
                                    .id(consumption.getDate_heure())
                                    .source(json, XContentType.JSON);
                            requestIndexer.add(indexRequest);
                        })
                        .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000)
                        .build()
        ).name("Elasticsearch Sink");


        // Execute program, beginning computation.
        env.execute("Flink éCO2mix nationales temps réel");
    }
}