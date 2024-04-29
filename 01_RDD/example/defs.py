import jieba


def split_word(data):
    return list(jieba.cut_for_search(data))


def filter_words(data):
    return data not in ["谷", "帮", "客"]


def append_words(data):
    if data == "传智播": data = "传智播客"
    if data == "院校": data = "院校帮"
    if data == "博学": data = "博学谷"
    return (data, 1)


def aggregation_user_count(data):
    "(id,data)"
    id = data[0]
    count = data[1]
    words = split_word(count)
    result = []
    for i in words:
        if filter_words(i):
            result.append((id + "_" + append_words(i)[0], 1))

    return result
