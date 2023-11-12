# Seismic events project

This project is the result of a week of development during our [data engineering](https://info.lewagon.com/en/data-engineering) bootcamp [@LeWagon](https://github.com/lewagon).

## Contributors
<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

## Why seismic events ?

### Information

A seismic events is not necessary an earthquake. It can happens for several reason like ice quake, avalanche etc.

With the recent earthquakes in earth we wanted to provide an application that can help local authorities (rescue services, ...) to determine how much people will be impacted by an earthquake to be able to act fast.
Among that we want to have a dashboard (Metabase) that help users to understand and track seismic events.

## Our data

We used USGS api to gather all the data needed.

## Data engineer stack

### Global view

![](images/stack.png)

### ELT

![](images/elt.png)

### PUB/SUB

![](images/pub_sub.png)

### Metabase dashboard

#### All seismic events since 1949

![](images/all_seismic_events.png)

#### Detail of a specific event

![](images/detail_of_a_seismic_event.png)
