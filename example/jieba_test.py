#!/usr/bin/python
# -*- coding:UTF-8 -*-
import jieba


def jieba_demo():
    content = "国务院总理李强7月18日上午在人民大会堂会见美国总统气候问题特使克里。李强指出，希望中美双方继续秉持合作精神，尊重彼此核心关切，通过充分沟通求同存异，探讨更加务实的机制化合作，推动多边气候治理进程，确保《巴黎协定》全面有效实施。"
    result = jieba.cut(content, True)
    print(list(result))
    print(type(result))

    result_2 = jieba.cut(content, False)
    print(list(result_2))
    print(type(result_2))
    # 搜索引擎模式
    result_3 = jieba.cut_for_search(content)
    print(','.join(result_3))


if __name__ == '__main__':
    jieba_demo()