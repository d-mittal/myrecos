import MySQLdb
import math
import cPickle as pickle
import random
import datetime as dt


DB1 = {
'MY_HOST':'180.179.145.21',
'MY_USER':'devashishmittal',
'MY_PASS':'6ad43d397e5858b79',
'MY_DB':'bidb',
'MY_PORT':5516  
}

TXTFILE_DIR = "E:\\Project_Files\\RecommenderSystems\\mailerRecs\\txt\\"


INPUT_DATE = str(dt.date.strftime(dt.datetime.now(),'%Y%m%d'))
styleDict = pickle.load(open(TXTFILE_DIR+"dimProduct.p","rb"))


def vectorize(style_id, styleDict):
    
    pv = {}
    style_descriptors = ['attrs','brand', 'colour', 'gender','price_bin','prod_dscnt']
    
    prodInfo = styleDict[style_id]
    
    at = prodInfo['article_type']+"|"+prodInfo['gender']
    
    if not pv.has_key(at):
        pv.setdefault(at,{})
        pv[at]['brand'] = {}
        #pv[at]['gender'] = {}
        pv[at]['attrs'] = {}
        pv[at]['price'] = {}
        pv[at]['prod_dscnt'] = {}
        pv[at]['colour'] = {}
        pv[at]['styleName'] = {}
    
    pv['subcategory'] = prodInfo['subcategory']
    pv['mastercategory'] = prodInfo['mastercategory']
    pv['article_type'] = at
    pv['gender'] = prodInfo['gender']
    pv['mrp'] = prodInfo['mrp']
    pv['prodDiscount'] = prodInfo['prod_dscnt']
    pv['asp'] = prodInfo['asp']
    
    # article type brands
    brandName = prodInfo['brand']
    pv[at]['brand'].setdefault(brandName,0)
    pv[at]['brand'][brandName] += 1
    
    # article type gender
    #genderName = prodInfo['gender']
    #pv[at]['gender'].setdefault(genderName,0)
    #pv[at]['gender'][genderName] += 1
    
    # article type price bins
    priceBinName = prodInfo['price_bin']
    pv[at]['price'].setdefault(priceBinName,0)
    pv[at]['price'][priceBinName] += 1
    
    # article type discount percent bins
    dscntBinName = prodInfo['prod_dscnt']
    pv[at]['prod_dscnt'].setdefault(dscntBinName,0)
    pv[at]['prod_dscnt'][dscntBinName] += 1
    
    # article type colour
    colorDict = prodInfo['colour']
    colorCount = len(colorDict)
    for colorType, colorVal in colorDict.items():
        pv[at]['colour'].setdefault(colorType, {}).setdefault(colorVal,0)
        pv[at]['colour'][colorType][colorVal] += 1.0
    
    # article type styleName
    styleName = prodInfo['styleName']
    styleName = styleName.replace(" &", "")
    styleName = styleName.replace(brandName,"")
    sn = styleName.split()
    #if len(sn)<3:
    ngrams = [' '.join(sn[i:i+2]) for i in range(len(sn) - 1)]
    #else:
    #ngrams = [' '.join(sn[i:i+3]) for i in range(len(sn) - 2)]
        
    for token in ngrams:
        pv[at]['styleName'].setdefault(token,0)
        pv[at]['styleName'][token] += 1
    
    try:
        # article type attributes
        attrDict = prodInfo['attrs']
        attrCount = len(attrDict)
        for attrName, attrVal in attrDict.items():
            pv[at]['attrs'].setdefault(attrName, {}).setdefault(attrVal,0)
            pv[at]['attrs'][attrName][attrVal] += 1.0
    except:
        return pv
        
    return pv
    
  
def prodVectorNormalization(pvec):
    
    at = pvec['article_type']
    s = 0
    normed_uv = {}
    normed_uv['subcategory'] = pvec['subcategory']
    normed_uv['mastercategory'] = pvec['mastercategory']
    normed_uv['article_type'] = pvec['article_type']
    normed_uv['gender'] = pvec['gender']
    normed_uv['mrp'] = pvec['mrp']
    normed_uv['prodDiscount'] = pvec['prodDiscount']
    normed_uv['asp'] = pvec['asp']
    
    normed_uv[at] = flatten(pvec[at])
    
    for i in normed_uv[at].values():
        s += i**2
    
    for k,v in normed_uv[at].items():
        normed_uv[at][k] = v/math.sqrt(s)
    
    return normed_uv


def writeSQLOutputToFile(fn, DB, query):
    
    inpFile = file(fn,'wb')
    
    db = MySQLdb.connect(host=DB['MY_HOST'],user=DB['MY_USER'],passwd = DB['MY_PASS'], db = DB['MY_DB'], port = DB['MY_PORT'])
    cur = db.cursor()
    cur.execute(query)
    numrows = int(cur.rowcount)
    
    for x in range(0,numrows):
        row = cur.fetchone()
        for col in row:
            inpFile.write("%s\t" % col)
        inpFile.write("\n")

    cur.close()
    db.close()
    inpFile.close()


def flatten(d, parent_key=''):
        
    items = []
    for k, v in d.items():
        try:
            items.extend(flatten(v, '%s%s_' % (parent_key, k)).items())
        except AttributeError:
            items.append(('%s%s' % (parent_key, k), v))
            
    return dict(items)
    

fn = TXTFILE_DIR+'liveStylesOn'+INPUT_DATE+'.txt'

#query = """
#select distinct a.style_id, a.season_code, count(distinct a.sku_id) as sku_count, 
#from bidb.dim_product a, bidb.fact_product_snapshot b, bidb.fact_product_snaphot c
#where
#    a.style_id = b.style_id
#and (a.article_mrp != 'None' and a.article_mrp > 0)
#and a.article_type is not null
#and (a.style_name is not null and a.style_name != '')
#and b.is_live_on_portal = 1
#and a.style_id = c.style_id
#and a.sku_id = b.sku_id
#and a.sku_id = c.sku_id
#and b.date = %s
#and c.date = %s
#""" % INPUT_DATE


query="""select b.style_id,  a.sku_id,b.is_live_on_portal, b.is_top_seller_style, sum(a.actual_net_inventory_count) as inv_count
from fact_inventory_count a, fact_product_snapshot b
where a.date = %s
and a.sku_id = b.sku_id
and a.date = b.date
group by b.style_id, b.sku_id, b.is_live_on_portal, b.is_top_seller_style""" % INPUT_DATE

writeSQLOutputToFile(fn, DB1, query)


stylesFile = file(fn,'rb')
#liveStyles = [style_id.strip() for style_id in stylesFile]

styleSummary = {}
for line in stylesFile:
    
    style_id, sku_id, is_live_on_portal, is_top_seller_style, inv_count = line.rstrip().split("\t")
    
    is_live_on_portal = int(is_live_on_portal)
    inv_count = int(inv_count)
    is_top_seller_style = int(is_top_seller_style)
    
    styleSummary.setdefault(style_id,{}).setdefault('is_live_on_portal',{}).setdefault(is_live_on_portal,[])
    styleSummary.setdefault(style_id,{}).setdefault('inv_count',0)
    styleSummary.setdefault(style_id,{}).setdefault('is_top_seller_style',0)
    
    styleSummary[style_id]['is_live_on_portal'][is_live_on_portal].append(sku_id)
    styleSummary[style_id]['inv_count'] += inv_count
    styleSummary[style_id]['is_top_seller_style'] = is_top_seller_style
    
stylesFile.close()

liveStyles = {}
for style_id in styleSummary: 

    liveSkus = 0
    nonliveSkus = 0
    
    if styleSummary[style_id]['is_live_on_portal'].has_key(1) and styleSummary[style_id]['is_live_on_portal'].has_key(0):
        liveSkus = len(styleSummary[style_id]['is_live_on_portal'][1])
        nonLiveSkus = len(styleSummary[style_id]['is_live_on_portal'][0])
        
    elif styleSummary[style_id]['is_live_on_portal'].has_key(1):
        liveSkus = len(styleSummary[style_id]['is_live_on_portal'][1])
        nonLiveSkus = 0
    elif styleSummary[style_id]['is_live_on_portal'].has_key(0):
        liveSkus = 0
        nonLiveSkus = len(styleSummary[style_id]['is_live_on_portal'][0])

    inv_count = styleSummary[style_id]['inv_count']
    is_top_seller_style = styleSummary[style_id]['is_top_seller_style']
    
    totalSkus = liveSkus + nonLiveSkus
    
    if totalSkus == 0: continue
    
    percLiveSkus = float(liveSkus)/totalSkus
    
    if percLiveSkus >=0.3:
        liveStyles[style_id] = is_top_seller_style
    

print len(liveStyles)

#liveStylesLst = sorted(liveStyles, key=liveStyles.__getitem__, reverse=True)


prodVector = file(TXTFILE_DIR+'prod_Vector_Signatures'+INPUT_DATE+'.txt','wb')


prodDict = {}

for style_id in liveStyles:
    
    if style_id not in styleDict: continue
    
    try:    
        pv = vectorize(style_id, styleDict)
    except:
        continue
    
    pv_norm = prodVectorNormalization(pv)
    
    p = repr([pv_norm,pv])
    
    prodDict[style_id] = [pv_norm,pv]
    
    prodVector.write(style_id + "\t" + p + "\n")
    

pickle.dump(prodDict,open(TXTFILE_DIR+"prod_Vector_Signatures.p","wb"))
prodVector.close()
