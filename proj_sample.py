from boilers.heatloadsuppling import StandardBoiler


if __name__ == '__main__':
    boiler = StandardBoiler('boilers/configs/boiler_config_200001.config')
    boiler.main()
