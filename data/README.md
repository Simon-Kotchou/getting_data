# COVID-19 Open Source Cough Dataset

<p align="center">
  <img width="300" src="http://virufy.org/images/logo.png">
</p>

## Why?
The COVID-19 pandemic is extremely detrimental to society and we need to all work together to overcome it. AI needs big data to work and we are unaware of any open source COVID-19 cough datasets anywhere in the world. We want to benefit humanity and have decided to be pioneers in open sourcing the first free COVID-19 cough dataset. This will spur innovation and collaboration from academia and industry all over the world.

## Data Source
Our data is very high accurate because it was collected at a hospital under supervision by physicians following Standard Operating Procedures (SOP). Our data is preprocessed and labeled with COVID-19 status (acquired from PCR testing), along with patient demographics (age, gender, medical history).

## Get Started
We have provided 121 segmented cough samples. The data is labeled with COVID-19 PCR test status, along with patient demographics as can be found in
[labels.csv](labels.csv)
 
## Structure
The data directory contains two different folders, one with the original cough audio recordings, and the other with the segmented versions of the coughs. The segmented coughs were created by identifying periods of relative silence in the recordings and separating coughs based on those silences. The segments which were not coughs or had too much background noise are removed.

## Collaboration
We warmly welcome review of our data and code. We hope to create a community of researchers driven to use this data to create solutions for the pandemic.

## More details
Please see our website http://virufy.org/data.
