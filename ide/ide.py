from importd import d

@d("/")
def index(request):
    return 'ide.html'

if __name__ == "__main__":
    d.main()