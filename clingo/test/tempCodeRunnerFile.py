for j in p:
            if type(jent).__name__ == 'dict':
                if j[0] not in jent:
                    jent[j[0]] = [j[1]]
                elif j[1] not in jent[j[0]]:
                    jent[j[0]].append(j[1])
            elif type(jent).__name__ == 'list':
                if j[0] not in jent:
                    jent.append(j[0])