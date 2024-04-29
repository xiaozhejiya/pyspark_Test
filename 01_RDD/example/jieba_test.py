# coding:utf8
import jieba

if __name__ == '__main__':
    content = "小明今天去广州吃饭"

    result = jieba.cut(content, True)
    print(list(result))
    print(type(result))

    result2 = jieba.cut(content, False)
    print(list(result2))

    result3 = jieba.cut_for_search(content)
    print(list(result3))