import argparse
from os import environ
from pymongo import MongoClient
from pyzipcode import ZipCodeDatabase


def printUsers(users, func=lambda x: x):
    for user in users:
        func(user)
        print('{0}: {1}'.format(user['profile']['name'], user['emails'][0]['address']))
    print('Total = {0}'.format(users.count()))


def saveUsers(users, filename, func=lambda x: x):
    with open(filename, 'w+') as fp:
        for user in users:
            func(user)
            fp.write('{0}, {1}\n'.format(user['profile']['name'], user['emails'][0]['address']))


def list_users(db, collection, state, out_file):
    if (state == 'registered'):
        users = db[collection].find({}, {'emails': 1, 'profile.name': 1})
    elif (state == 'accepted'):
        users = db[collection].find({'settings.accepted.flag': True},
                                    {'emails': 1, 'profile.name': 1})
    elif (state == 'confirmed'):
        users = db[collection].find({'settings.confirmed.flag': True},
                                    {'emails': 1, 'profile.name': 1})
    else:
        return
    if (out_file):
        saveUsers(users, out_file)
    else:
        printUsers(users)


def accept_user(db, collection, doc_id, travel, reimbursement):
    db[collection].update_one({'_id': doc_id}, {
         '$set': {
            'settings.accepted.flag': True,
            'settings.accepted.travel': {
               'method': travel,
               'reimbursement': reimbursement
            }
         }
      }
    )


def accept_all_in_region(db, collection, zipcode, radius, travel_method, reimburse_val, out_file,
                         overwrite=False):
    zcdb = ZipCodeDatabase()
    zips = [z.zip for z in zcdb.get_zipcodes_around_radius(zipcode, radius)]
    if (overwrite):
        query = {'profile.travel.zipcode': {'$in': zips}}
    else:
        query = {'profile.travel.zipcode': {'$in': zips}, 'settings.accepted.flag': False}
    users = db[collection].find(query, {'_id': 1, 'profile.name': 1, 'emails': 1})
    tot = users.count()

    def func(user):
        accept_user(db, collection, user['_id'], travel_method, reimburse_val)
    if (out_file):
        saveUsers(users, out_file, func)
    else:
        printUsers(users, func)
    print('Accepted {0} user{1} witin {2} miles of {3}'.format(tot, 's' if tot != 1 else '',
                                                               radius, zipcode))


def accept_all_at_school(db, collection, school_name, travel_method, reimburse_val, out_file,
                         overwrite=False):
    if (overwrite):
        query = {'profile.school': school_name}
    else:
        query = {'profile.school': school_name, 'settings.accepted.flag': False}
    users = db[collection].find(query, {'_id': 1, 'profile.name': 1, 'emails': 1})
    tot = users.count()

    def func(user):
        accept_user(db, collection, user['_id'], travel_method, reimburse_val)
    if (out_file):
        saveUsers(users, out_file, func)
    else:
        printUsers(users, func)
    print('Accepted {0} user{1} at {2}'.format(tot, 's' if tot != 1 else '',
                                               school_name))


def main(command, subcommand, db, database, collection, out_file, options):
    client = MongoClient(db)
    db = client[database]
    if (command == 'list'):
        list_users(db, collection, subcommand, out_file)
    elif (command == 'accept'):
        if (subcommand == 'school'):
            accept_all_at_school(db, collection, options['school_name'], options['travel_method'],
                                 options['reimburse_val'], out_file, options['overwrite'])
        elif (subcommand == 'region'):
            accept_all_in_region(db, collection, options['zipcode'], options['radius'],
                                 options['travel_method'], options['reimburse_val'], out_file,
                                 options['overwrite'])

if (__name__ == '__main__'):
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['list', 'accept'])
    parser.add_argument('subcommand', nargs='?', choices=['registered', 'accepted', 'confirmed',
                                                          'school', 'region'])
    parser.add_argument('--db', default=environ.get('MONGO_URL'))
    parser.add_argument('--database', '-d')
    parser.add_argument('--collection', '-c', default='users')
    parser.add_argument('--out', '-o')
    parser.add_argument('--school', '-s')
    parser.add_argument('--travel_method', '-t')
    parser.add_argument('--reimburse_val', '-r')
    parser.add_argument('--zipcode', '-z')
    parser.add_argument('--radius', '-rd', type=int)
    parser.add_argument('--overwrite', '-w', type=bool, default=False)
    args = parser.parse_args()

    options = {
        'school_name': args.school,
        'travel_method': args.travel_method,
        'reimburse_val': args.reimburse_val,
        'overwrite': args.overwrite,
        'zipcode': args.zipcode,
        'radius': args.radius
    }

    main(args.command, args.subcommand, args.db, args.database, args.collection, args.out, options)
