import requests
import beautifulsoup4
import csv


def get_html(url):
    r = requests.get(url)
    if r.ok:  # 200  ## 403 404
        return r.text
    print(r.status_code)


def refine_cy(s):
    return s.split(' ')[-1]


def write_csv(data):
    with open('url_ID.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow((data['id'],
                         data['url'],
                         data['date']))


def get_page_data(html):
    soup = BeautifulSoup(html, 'lxml')
    lws = soup.find_all('div', class_='obj_item click_open')
    #     lws = soup.find_all('div', class_='obj_item click_open first_item')
    #     lws = soup.find_all('div', class_='obj_item click_open last_item')

    for lw in lws:
        try:
            lw_id = lw.find('div', class_='favor_table').find('div', class_="favor_table_td2").find('div',
                                                                                                    class_='o_top').find(
                'span', class_='o_num').text
        except:
            lw_id = ''

        try:
            date = \
            lw.find('div', class_='favor_table').find('div', class_="favor_table_td2").find('div', class_='o_top').find(
                'span', class_='o_date o_date_div').text.split(' ')[-1]
        except:
            date = ''

        data = {'id': lw_id.replace('В архиве', '').strip(),
                'date': date.replace('В архиве', '').strip(),
                'url': 'https://sozd.duma.gov.ru/bill/' + lw_id.replace('В архиве', '').strip()}

        write_csv(data)


def main():
    pattern = 'https://sozd.duma.gov.ru/oz?b%5BNumberSpec%5D=&b%5BAnnotation%5D=&b%5BConvocation%5D%5B%5D=7&b%5BConvocation%5D%5B%5D=6&date_period_from_Year=&date_period_to_Year=&b%5BYear%5D=&cond%5BClassOfTheObjectLawmaking%5D=any&cond%5BThematicBlockOfBills%5D=any&cond%5BPersonDeputy%5D=any&cond%5BFraction%5D=any&b%5BFzNumber%5D=&b%5BNameComment%5D=&b%5BResolutionnumber%5D=&cond%5BRelevantCommittee%5D=any&b%5BfirstCommitteeCond%5D=and&cond%5BResponsibleCommittee%5D=any&b%5BsecondCommitteeCond%5D=and&cond%5BHelperCommittee%5D=any&cond%5BExistsEvents%5D=any&date_period_from_ExistsEventsDate=&date_period_to_ExistsEventsDate=&b%5BExistsEventsDate%5D=&cond%5BLastEvent%5D=any&date_period_from_MaxDate=&date_period_to_MaxDate=&b%5BMaxDate%5D=&cond%5BExistsDecisions%5D=any&date_period_from_DecisionsDateOfCreate=&date_period_to_DecisionsDateOfCreate=&b%5BDecisionsDateOfCreate%5D=&cond%5BLastDecisions%5D=any&cond%5BQuestionOfReference%5D=any&cond%5BSubjectOfReference%5D=any&b%5BconclusionRG%5D=&date_period_from_dateEndConclusionRG=&date_period_to_dateEndConclusionRG=&b%5BdateEndConclusionRG%5D=&cond%5BFormOfTheObjectLawmaking%5D=any&cond%5BinSz%5D=any&date_period_from_ResponseDate=&date_period_to_ResponseDate=&b%5BResponseDate%5D=&date_period_from_AmendmentsDate=&date_period_to_AmendmentsDate=&b%5BAmendmentsDate%5D=&b%5BSectorOfLaw%5D=&b%5BClassOfTheObjectLawmakingId%5D=34f6ae40-bdf0-408a-a56e-e48511c6b618#data_source_tab_b'
    for i in range(1, 2957):
        url = pattern.format(str(i))
        get_page_data(get_html(url))


if __name__ == '__main__':
    main()