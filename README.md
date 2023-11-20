# Seismic events project

This project is the result of a week of development during our [data engineering](https://info.lewagon.com/en/data-engineering) bootcamp [@LeWagon](https://github.com/lewagon).

## Contributors

<table>
   <tbody>
     <tr>
       <td align="center" valign="top" width="14.28%"><a href="https://github.com/messaoudia"><img src="https://avatars.githubusercontent.com/u/4719434?v=4?s=100" width="100px;" alt="Amin Messaoudi"/><br /><sub><b>Amin Messaoudi</b></sub></a><br /><a href="https://github.com/batch1413-earthquake/earthquake-project/commits?author=messaoudia" title="Code">💻</a></td>
       <td align="center" valign="top" width="14.28%"><a href="https://github.com/jalandet"><img src="https://avatars.githubusercontent.com/u/40422792?v=4?s=100" width="100px;" alt="jalandet"/><br /><sub><b>Juan Alandete</b></sub></a><br /><a href="https://github.com/batch1413-earthquake/earthquake-project/commits?author=jalandet" title="Code">💻</a></td>
       <td align="center" valign="top" width="14.28%"><a href="https://github.com/maxdelob"><img src="https://avatars.githubusercontent.com/u/8874607?v=4?s=100" width="100px;" alt="maxdelob"/><br /><sub><b>Maxime Delobelle</b></sub></a><br /><a href="https://github.com/batch1413-earthquake/earthquake-project/commits?author=maxdelob" title="Code">💻</a></td>
       <td align="center" valign="top" width="14.28%"><a href="https://github.com/jlsrvr"><img src="https://avatars.githubusercontent.com/u/24507606?v=4" width="100px;" alt="jlsrvr"/><br /><sub><b>Jules Rivoire</b></sub></a><br /><a href="https://github.com/batch1413-earthquake/earthquake-project/commits?author=jlsrvr" title="Code">💻</a></td>
     </tr>
   </tbody>
 </table>

## Why seismic events ?

### Information

A seismic event is not necessarily an earthquake. It can happen for several reasons like an ice quake, an explosion etc.

### Our goal

With the recent earthquakes on earth we wanted to provide an application that would help local authorities (rescue services, ...) to determine how many people will be impacted by a seismic event to be able to act fast.
For that we wanted to have a dashboard (Metabase) that would help users to understand and track seismic events.

## Our data

We extracted the data from the USGS api on seismic events.

## Data engineer stack

### Global view

![](images/stack.png)

### ELT (Extract|Load|Transform)

![](images/elt.png)

- **E**:Extract data from USGS and upload to our bronze layer

<details open>
<summary>Extract dag</summary>
<br>

![](images/extract.png)
</details>

- **L**:Load data and clean it to store it in our silver layer

<details open>
<summary>Load dag</summary>
<br>

![](images/load.png)
</details>

- **T**:Transform data to upload it to our gold layer on Google BigQuery

<details open>
<summary>Transform dag</summary>
<br>

![](images/transform.png)
</details>

### PUB/SUB

![](images/pub_sub.png)

> The Gmail alerting has not yet been developed

### Metabase dashboard

#### All seismic events since december 1949

![](images/all_seismic_events.png)

#### Detail of a specific event

![](images/detail_of_a_seismic_event.png)
