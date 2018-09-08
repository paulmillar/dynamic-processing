/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.kafka.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 *
 */
public class EventGenerators
{
    private final List<EventGenerator> generators;

    public EventGenerators(List<Configuration.EventSource> eventSources,
            UrlGenerator urlGenerator)
    {
        generators = eventSources.stream()
                .map(g -> new EventGenerator(g, urlGenerator))
                .collect(Collectors.toList());
    }

    public boolean matches(String path)
    {
        return generators.stream().anyMatch(g -> g.matches(path));
    }

    public List<String> eventsFor(String path)
    {
        List<String> l = new ArrayList<>();

        for (EventGenerator g : generators) {
            g.map(path).ifPresent(l::add);
        }

        return l;
    }
}
