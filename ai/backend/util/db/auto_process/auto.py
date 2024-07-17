from ai.backend.util.db.auto_process.create_new_sp_ad_auto import Ceate_new_sku

def auto_process():
    api = Ceate_new_sku()
    #api.create_new_sp_auto_no_template1('US',['L59', 'M121', 'M23', 'M118', 'M108', 'G11', 'L96', 'M103', 'L103', 'M82', 'M131'],'LAPASA')
    api.create_new_sp_auto_no_template('IT',['B01IH50USQ', 'B07N1YJBYP', 'B0B6G649YT', 'B0B6HJQ5SR', 'B0B6HS38JB', 'B0BWY4CWDW', 'B0C7G7GRB1', 'B0CCPDBX8K', 'B0CCY235RC', 'B0CCYLSV5Y', 'B0CDPV7D5D', 'B0CG1GN1NT', 'B0CTHL8K2X'],'OutdoorMaster')

auto_process()

