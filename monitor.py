import pymongo, datetime

def safe_count(col, filter):
    try: return col.count_documents(filter)
    except: return '?'

try:
    s = pymongo.MongoClient('mongodb://localhost:27018/', serverSelectionTimeoutMS=3000)['bce_state_db']['download_state']
    csv_done = safe_count(s, {'source':'nbb_csv','status':'done'})
    pdf_done = safe_count(s, {'source':'nbb_pdf','status':'done'})
    pdf_err  = safe_count(s, {'source':'nbb_pdf','status':'error'})
    pdf_pend = safe_count(s, {'source':'nbb_pdf','status':'pending'})
except Exception as e:
    csv_done = pdf_done = pdf_err = pdf_pend = 'offline'

try:
    db = pymongo.MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=3000)['bce_db']
    ent = db['kbo_enterprises'].count_documents({})
    act = db['kbo_activities'].count_documents({})
except: ent = act = '?'

print(datetime.datetime.now().strftime('%H:%M:%S'),
      '| CSV:', csv_done, '| PDF done:', pdf_done, '| PDF err:', pdf_err,
      '| enterprises:', ent, '| activities:', act)
