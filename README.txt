Impala + Tableau
2017/02/19 : RS initial remarks

- To track campaign performance by device and by interest (Forum) on Kaidee, Pantip and Facebook. For Facebook, we can track only from the landing pages.

- The table dtac_vertical_dataset is not yet used here seeing that the header_url with must be used to see on which forum/ room (Kaidee and Pantip) the targets are are not yet available. (I already raised this issue to Tapad's engineers on a video meeting on Fri 17 feb 2017)

- When the header_url is ready, we should move to use dtac_vertical_dataset instead seeing that its carrier are more accurate (it updates the ip address range for each carrier frequently)