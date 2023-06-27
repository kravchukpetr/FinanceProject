import datetime

def main():
    try:
        # Getting the current time upto seconds only.
        cur_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('Current Time : ',cur_time)
    except:
        print('Something Wrong in main function',exc_info=True)

if __name__=='__main__':
    main()
