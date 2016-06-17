f = open('smartbase_meta_sample.xml')
lines = f.readlines()
i = 0
count_line = []
rule_print = []
for line in lines:
    line = line.strip().split('>')
    if line[0] == '<rules' or line[0] == '</rules':
        count_line.append(i)
    i = i + 1

len_count = len(count_line)

for j in range(len_count):
    if j % 2 == 0:
        temp = ''
        for data_line in range(count_line[j], count_line[j + 1]):
            if count_line[j] + 1 == count_line[j + 1]:
                temp = temp + 'none'
            else:
                line = lines[data_line]
                line = line.strip().split('>')
                if line[0] in ['<oper1', '<expr1', '<expr2']:
                    temp = temp + line[1].split('<')[0]
        rule_print.append(temp)

for data in rule_print:
    print data + '\n'
