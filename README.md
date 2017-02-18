# PageRank
Method to calculate page rank:
The assignment asks us to calculate page rank for 8 iterations. Firstly, I have initialized the rank of every page by 1/N. 
This is a map reduce job. The reducer will output the pages in a list format where there will be {title, initialized rank, output link}. 
This is my iteration 0. This output is stored in the temporary folder for next phase calculations. 
For the actual 8 iterations, I have splitted these title, rank and output link. 
Further the process of counting the output links and the rank weights is done. 
Rank weight is the (rank/output link count) for current page of all output links. 
The mapper emits this weight to the reducer which adds up all the weights from all the links and calculates the page rank. 
After calculation page rank is displayed in the format {title, rank, output links}
R(i) = (1 - d)/N + Σ((R(j)/L(j)) where j belongs to M(i)
d = damping factor
R(j) = page rank of page j
N = total number of pages
L(j) = total number of output links on page j
