from argparse import ArgumentParser

from sqlite.database import Database


def main():
    parser = ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('command')
    args = parser.parse_args()

    db = Database(args.filename)

    command = args.command.lower()
    match command:
        case '.dbinfo':
            db.info()
        case '.tables':
            db.print_tables()
        case _ if command.startswith('select count(*) from'):
            db.count_table(command)
        case _:
            print('Invalid command: ' + args.command)

    db.close()


if __name__ == '__main__':
    main()
