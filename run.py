import uvicorn
import argparse


argparse = argparse.ArgumentParser(
    description="DataCell Server"
)

argparse.add_argument(
    '-H', '--host',
    type=str,
    default='127.0.0.1',
    help='Server host'
)

argparse.add_argument(
    '-p', '--port',
    type=int,
    default=8168,
    help='Server port'
)

argparse.add_argument(
    '-r', '--reload',
    type=bool,
    default=False,
    help='Server reload'
)

args = argparse.parse_args()


if __name__ == '__main__':
    
    print(args.reload)

    uvicorn.run(
        'app.main:app', 
        host = args.host,
        port = args.port, 
        reload = args.reload,
    )