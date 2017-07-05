#!/usr/bin/env python
from optparse import OptionParser
import MySQLdb
import csv

parser = OptionParser()

parser.add_option("-p", "--path", dest="path",
                  help="""Path to the CSV file to parse (make sure only entries to DB should be present there)
                    Format should be three columns, comma separated, in the following order: category, key, translation.
                    (Provide with "" if has spaces in path name)
                  """, metavar="FILE")
parser.add_option("-u", "--update",
                  action="store_true", dest="should_update", default=False,
                  help="Whether to update existing records with provided translatios, default to False")
parser.add_option("-s", "--staging",
                  action="store_true", dest="staging", default=False,
                  help="""If present, will connect and work with the staging DB
                  host: zazmamessages.cu2ovaqnaz1u.us-east-1.rds.amazonaws.com
                  """)

parser.add_option("-t", "--target-host",
                  default="127.0.0.1",
                  help="""If present, will connect and work with the specified DB host
                  """)


(options, args) = parser.parse_args()
if not options.path:
    print "\nNeed a valid CSV path, need to work with something here young Grasshopper!\n"
    exit()
else:
    csv_path = options.path

SELECT_SOURCE_MESSAGE_SCRIPT = 'SELECT * FROM SourceMessage WHERE category = "%s" AND message = "%s"'
SELECT_MESSAGE_SCRIPT = 'SELECT * FROM Message WHERE id = %s'
INSERT_NEW_SOURCE_MESSAGE_SCRIPT = 'INSERT INTO SourceMessage (id, category, message) VALUES ("%s", "%s", "%s")'
INSERT_NEW_TRANSLATION = 'INSERT INTO Message VALUES (%s, "en_US", "%s");'
UPDATE_TRANSLATION = 'UPDATE Message SET translation = "%s" WHERE id = %s;'


if options.staging:
    db = MySQLdb.connect(host="zazmamessages.cu2ovaqnaz1u.us-east-1.rds.amazonaws.com",
                         user="zazma_website",
                         passwd="YqMvAftZyjEqgox",
                         db="zazmamessages")
else:
    db = MySQLdb.connect(host=options.target_host,
                     user="root",
                     passwd="1234",
                     db="zazmamessages")


cur = db.cursor()


def source_message_table_max_id():
    cur.execute("SELECT MAX(id) FROM SourceMessage")
    return cur.fetchall()[0][0]


def get_current_record_id(category, key):
    cur.execute(SELECT_SOURCE_MESSAGE_SCRIPT % (category, key))
    return cur.fetchall()[0][0]


def get_message_by_id(message_id):
    cur.execute(SELECT_MESSAGE_SCRIPT % message_id)
    return cur.fetchall()


# true means message exists
def check_source_message_exists(category, key):
    cur.execute(SELECT_SOURCE_MESSAGE_SCRIPT % (category, key))
    record_exist = len(cur.fetchall()) > 0
    if record_exist:
        print '%s.%s Already exists' % (category, key)
    else:
        print '%s.%s Does not exist' % (category, key)
    return record_exist


# Add new message and return it's index
def insert_new_source_message(category, key):
    last_id = source_message_table_max_id()

    try:
        print 'inserting new source message:'
        print INSERT_NEW_SOURCE_MESSAGE_SCRIPT % (last_id + 1, category, key)
        cur.execute(INSERT_NEW_SOURCE_MESSAGE_SCRIPT % (last_id + 1, category, key))
        db.commit()
    except:
        print "error inserting source message\n\n"
        db.rollback()

    return last_id + 1


def create_translation(new_id, translation):
    try:
        print 'inserting new translation!'
        print INSERT_NEW_TRANSLATION % (new_id, translation)
        cur.execute(INSERT_NEW_TRANSLATION % (new_id, translation))
        db.commit()
    except:
        print "error inserting translation\n\n"
        db.rollback()


def update_translation(message_id, translation):
    try:
        print 'updating translation!'

        if len(get_message_by_id(message_id)) == 0:
            print "message doesn't exist, attempting insertion"
            create_translation(message_id, translation)
        else:
            print UPDATE_TRANSLATION % (translation, message_id)
            cur.execute(UPDATE_TRANSLATION % (translation, message_id))
            db.commit()
    except:
        print "error updating translation\n\n"
        db.rollback()


def pipeline():
    with open(csv_path, 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            category = row[0].strip('"')
            key = row[1].strip('"')
            translation = row[2].strip('"')
            message_exist = check_source_message_exists(category, key)

            if not message_exist:
                inserted_record_id = insert_new_source_message(category, key)
                if translation:
                    create_translation(inserted_record_id, translation)
                else:
                    create_translation(inserted_record_id, key)
            else:
                if translation and options.should_update:
                    update_translation(get_current_record_id(category, key), translation)


pipeline()

db.close()
