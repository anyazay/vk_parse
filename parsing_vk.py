import pandas as pd
import vk
from collections import defaultdict
from time import sleep
import math 

session = vk.Session(access_token='your_wonderful_token')
api = vk.API(session, v='5.35', lang='ru', timeout=10)

df = pd.DataFrame()

domains_1 = ['group1', 'group2', 'group3']

domains_2 = ['another_group1', 'another_group2']

domains_3 = ['one_more_group1', 'one_more_group2', 'one_more_group3']

domains = [domains_1, domains_2, domains_3]

count = 0

keywords = ['масленица', 'блин', 'икра', 'сгущенк', 'чай', 'чая', 'чаю', 'чучел']

# пробегаемся по каждому из типов доменов
for each in domains:

    if count == 0: # чтобы в датафрейме отделить разные типы доменов друг от друга
        typeofdomain = '1'
    elif count == 1:
        typeofdomain = '2'
    elif count == 2:
        typeofdomain = '3'

    def parse_vk_maslenitsa(domains, keywords, typeofdomain):
        print(typeofdomain)

        dataframe_of_posts = pd.DataFrame()

        for d in domains:
            list_of_posts = []
            ofst = 0
            r = 100
            cont = True
            try:
                while cont == True: # запускаем while до определенной даты поста — она будет ниже, в if
                    post = api.wall.get(domain=d, count=r, offset=ofst)
                    for k in keywords:
                        for p in post['items']:
                            if k in p['text']: # проверяем, содержит ли текст поста нужное ключевое слово
                                list_of_posts.append(p['text'])
                    sleep(3) # чтобы не задудосить. необязательно так делать, но я осторожничала
                    print(f'done with loop {ofst} - {r}')
                    ofst+=100
                    r+=100
                    if post['items'][0]['date'] <= 1514764800: # забираем за определенное время
                        cont = False
            except Exception as e:
                print(e)
                continue # некоторые группы могут быть моложе, чем нам надо, тогда возникает Exception
            timely_df = pd.DataFrame(set(list_of_posts)) # текст поста может несколько раз подойти по условию
            timely_df['domain'] = d # тут грязновато и на коленке
            timely_df['type'] = typeofdomain
            dataframe_of_posts = dataframe_of_posts.append(timely_df)
            print(f'done with {d}')    
        print('script ended okay!')
        return dataframe_of_posts

    df = df.append(parse_vk_maslenitsa(each, keywords, typeofdomain))
    
    count+=1


# достаем комментарии к некоторым из постов. и комментарии, и посты, должны отвечать критерию "содержит"
# одно или несколько ключевых слов

df_comments = pd.DataFrame()


domains =  ['group1', 'group2']

count = 0

keywords = ['масленица', 'блин', 'икра', 'сгущенк', 'чай', 'чая', 'чаю', 'чучел']

# функция получилась очень вложенной, но сама структура постов и комментариев к ним такая :(
# нужно было 1. использовать больше list comprehensions; 2. вложить нужную часть функции по комментариям в 
# функцию по постам, но я не хотела перегружать верхнюю функцию: я к ней часто обращалась и меняла вводные

def parse_vk_maslenitsa_comments(domains, keywords):
    print('start')

    dataframe_of_comments = pd.DataFrame()

    for d in domains:
        
        ofst = 0
        r = 100
        cont = True
        try:
            while cont == True:
                post = api.wall.get(domain=d, count=r, offset=ofst)
                for k in keywords:
                    for p in post['items']:
                        timely_df = pd.DataFrame()
                        if k in p['text']:
                            amount_of_comments = math.ceil(p['comments']['count'] / 100)
                            for ac in range(amount_of_comments):
                                comments = api.wall.getComments(owner_id=p['owner_id'], post_id=p['id'], need_likes=1,
                                                               offset=0, count=100)
                                comment_texts = [c['text'] for c in comments['items']]
                                comment_likes = [c['likes']['count'] for c in comments['items']]
                                timely_df = pd.DataFrame(comment_texts) # опять же, можно было сделать красивее
                                timely_df['likes'] = comment_likes
                                timely_df['domain'] = d
                                timely_df['post'] = p['text']
                                dataframe_of_comments = dataframe_of_comments.append(timely_df.drop_duplicates())
                sleep(3)
                print(f'done with loop {ofst} - {r}')
                ofst+=100
                r+=100
                if post['items'][0]['date'] <= 1514764800:
                    cont = False
        except Exception as e:
            print(e)
            continue
        print(f'done with {d}')    
    print('script ended okay!')
    return dataframe_of_comments.drop_duplicates()

df_comments = parse_vk_maslenitsa_comments(domains, keywords)