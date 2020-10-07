from pyspark import SparkConf, SparkContext

# This mapper is designed to turn input data into tuples
# Input format: "Matrix name(M/N)", "column index", "row index", "value"
# Output format: ("row index", (0, "column index", "value")) or ("column index", (1, "row index", "value"))
def mapper1(lines):
    info = lines.split(",")
    m_type = 0 if info[0] == 'M' else 1
    return (int(info[(m_type+1)%2+1]), (m_type, int(info[m_type+1]), int(info[3])))

# This mapper is designed to split data into a M matrix and a N matrix,
# then get the cartesian product of two matrice
# Since (j, (0, i, v0)) multiply (j, (1, k, v1)), will be part of answer (i, k)
# We should return data as ((i, j), v0*v1)
# Input format: (j, [(0, i, value_0)......(1, k, value_n)])
# Output format: [((i_0, k_0), value_0*value_1), ((i_1, k_1), value_2 * value_3)......]
def mapper2(data):
    m_mat = list(filter(lambda x: x[0] == 0, data[1]))
    n_mat = list(filter(lambda x: x[0] == 1, data[1]))
    mn_multiply = []
    for m in m_mat:
        for n in n_mat:
            mn_multiply.append(((m[1], n[1]), m[2]*n[2]))
    return mn_multiply

# Finally, sum data with same (i, j)
def reducer1(x, y):
    return x+y


sc = SparkContext.getOrCreate()
lines = sc.textFile("500input.txt").map(mapper1)
# group the M elements with same row index, and N elements with same column index
lines2 = lines.groupByKey()
lines3 = lines2.flatMap(mapper2)
lines4 = lines3.reduceByKey(reducer1).collect()

f = open("./Outputfile.txt", 'a')
for e in sorted(lines4):
    f.write('%d,%d,%d\n'%(e[0][0], e[0][1], e[1]))
f.close()
sc.stop()

