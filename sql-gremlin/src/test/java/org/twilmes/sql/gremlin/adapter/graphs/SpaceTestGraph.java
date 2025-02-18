package org.twilmes.sql.gremlin.adapter.graphs;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.structure.T.label;

public class SpaceTestGraph implements TestGraph {
    @Override
    public void populate(final Graph graph) {
        // companies
        final Vertex acmeSpaceCo = graph.addVertex(label, "company", "name", "Acme Space");
        final Vertex newFrontiers = graph.addVertex(label, "company", "name", "New Frontiers");
        final Vertex tomorrowUnlimited = graph.addVertex(label, "company", "name", "Tomorrow Unlimited");
        final Vertex spaceTruckers = graph.addVertex(label, "company", "name", "Space Truckers");
        final Vertex bankruptCo = graph.addVertex(label, "company", "name", "Bankrupt Co.");


        // planets
        final Vertex earth = graph.addVertex(label, "planet", "name", "earth");
        final Vertex mars = graph.addVertex(label, "planet", "name", "mars");
        final Vertex saturn = graph.addVertex(label, "planet", "name", "saturn");
        final Vertex jupiter = graph.addVertex(label, "planet", "name", "jupiter");

        // astronauts
        final Vertex tom = graph.addVertex(label, "person", "name", "Tom", "age", 35, "wentToSpace", false);
        final Vertex patty = graph.addVertex(label, "person", "name", "Patty", "age", 29, "wentToSpace", false);
        final Vertex phil = graph.addVertex(label, "person", "name", "Phil", "age", 31, "wentToSpace", false);
        final Vertex susan = graph.addVertex(label, "person", "name", "Susan", "age", 45, "wentToSpace", true);
        final Vertex juanita = graph.addVertex(label, "person", "name", "Juanita", "age", 50, "wentToSpace", true);
        final Vertex pavel = graph.addVertex(label, "person", "name", "Pavel", "age", 30, "wentToSpace", true);

        // spaceships
        final Vertex spaceship1 = graph.addVertex(label, "spaceship", "name", "Ship 1", "model", "delta 1");
        final Vertex spaceship2 = graph.addVertex(label, "spaceship", "name", "Ship 2", "model", "delta 1");
        final Vertex spaceship3 = graph.addVertex(label, "spaceship", "name", "Ship 3", "model", "delta 2");
        final Vertex spaceship4 = graph.addVertex(label, "spaceship", "name", "Ship 4", "model", "delta 3");

        // satellite
        final Vertex satellite1 = graph.addVertex(label, "satellite", "name", "sat1");
        final Vertex satellite2 = graph.addVertex(label, "satellite", "name", "sat2");
        final Vertex satellite3 = graph.addVertex(label, "satellite", "name", "sat3");

        // rocket fuel
        final Vertex s1Fuel = graph.addVertex(label, "sensor", "type", "rocket fuel");

        // astronaut company relationships
        tom.addEdge("worksFor", acmeSpaceCo, "yearsWorked", 5);
        patty.addEdge("worksFor", acmeSpaceCo, "yearsWorked", 1);
        phil.addEdge("worksFor", newFrontiers, "yearsWorked", 9);
        susan.addEdge("worksFor", tomorrowUnlimited, "yearsWorked", 4);
        juanita.addEdge("worksFor", spaceTruckers, "yearsWorked", 4);
        pavel.addEdge("worksFor", spaceTruckers, "yearsWorked", 10);

        // astronaut spaceship
        tom.addEdge("pilots", spaceship1);
        patty.addEdge("pilots", spaceship1);
        phil.addEdge("pilots", spaceship2);
        susan.addEdge("pilots", spaceship3);
        juanita.addEdge("pilots", spaceship4);
        pavel.addEdge("pilots", spaceship4);

        // astronauts to planets
        tom.addEdge("fliesTo", earth).property("trips", 10);
        tom.addEdge("fliesTo", mars).property("trips", 3);
        patty.addEdge("fliesTo", mars).property("trips", 1);
        phil.addEdge("fliesTo", saturn).property("trips", 9);
        phil.addEdge("fliesTo", earth).property("trips", 4);
        susan.addEdge("fliesTo", jupiter).property("trips", 20);
        juanita.addEdge("fliesTo", earth).property("trips", 4);
        juanita.addEdge("fliesTo", saturn).property("trips", 7);
        juanita.addEdge("fliesTo", jupiter).property("trips", 9);
        pavel.addEdge("fliesTo", mars).property("trips", 0);

        // astronaut friends
        tom.addEdge("friendsWith", patty);
        patty.addEdge("friendsWith", juanita);
        phil.addEdge("friendsWith", susan);
        susan.addEdge("friendsWith", pavel);

        // satellites to planets
        satellite1.addEdge("orbits", earth).property("launched", 1995);
        satellite2.addEdge("orbits", mars).property("launched", 2020);
        satellite3.addEdge("orbits", jupiter).property("launched", 2005);

        // fuel sensor readings
        long timestamp = 1765258774000L;
        for (int i = 0; i < 10; i++) {
            final Vertex s1Reading =
                    graph.addVertex(label, "sensorReading", "timestamp", timestamp, "date", timestamp, "value", 10.0);
            s1Fuel.addEdge("hasReading", s1Reading);
            timestamp += TimeUnit.MINUTES.toMillis(5);
        }
    }
}
