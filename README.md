# Seismic events project

This project is the result of a week of development during our [data engineering](https://info.lewagon.com/en/data-engineering) bootcamp [@LeWagon](https://github.com/lewagon).

## Contributors

<table>
   <tbody>
     <tr>
       <td align="center" valign="top" width="14.28%"><a href="https://github.com/messaoudia"><img src="https://avatars.githubusercontent.com/u/4719434?v=4?s=100" width="100px;" alt="Amin Messaoudi"/><br /><sub><b>Amin Messaoudi</b></sub></a><br /><a href="https://github.com/batch1413-earthquake/earthquake-project/commits?author=messaoudia" title="Code">💻</a></td>
       <td align="center" valign="top" width="14.28%"><a href="https://github.com/jalandet"><img src="https://avatars.githubusercontent.com/u/40422792?v=4?s=100" width="100px;" alt="jalandet"/><br /><sub><b>Juan Alandete</b></sub></a><br /><a href="https://github.com/batch1413-earthquake/earthquake-project/commits?author=jalandet" title="Code">💻</a></td>
       <td align="center" valign="top" width="14.28%"><a href="https://github.com/maxdelob"><img src="https://avatars.githubusercontent.com/u/8874607?v=4?s=100" width="100px;" alt="maxdelob"/><br /><sub><b>Maxime Delobelle</b></sub></a><br /><a href="https://github.com/batch1413-earthquake/earthquake-project/commits?author=maxdelob" title="Code">💻</a></td>
       <td align="center" valign="top" width="14.28%"><a href="https://github.com/jlsrvr"><img src="https://avatars.githubusercontent.com/u/24507606?v=4" width="100px;" alt="jlsrvr"/><br /><sub><b>Jules Rivoir</b></sub></a><br /><a href="https://github.com/batch1413-earthquake/earthquake-project/commits?author=maxdelob" title="Code">💻</a></td>
     </tr>
   </tbody>
 </table>

## Why seismic events ?

### Information

A seismic events is not necessary an earthquake. It can happens for several reason like ice quake, avalanche etc.

### Our goal

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
