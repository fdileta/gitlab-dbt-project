echo "Downloading python 3.10.3"
curl https://www.python.org/ftp/python/3.10.3/python-3.10.3-macos11.pkg >> python-3.10.3-macos11.pkg

echo "Installing python 3.10.3"
sudo installer -pkg python-3.10.3-macos11.pkg -target /

echo "Setting up SSL links for your new version of python"
ln -s /etc/ssl/* /Library/Frameworks/Python.framework/Versions/3.10/etc/openssl

echo "Setting up path links for your new version of python"
export PATH="$HOME/Library/Python/3.10/bin":"$PATH" >> ~/.zshrc
export PATH=$HOME/bin:/usr/local/bin:$PATH >> ~/.zshrc
export PATH="$HOME/.poetry/bin:$PATH" >> ~/.zshrc

rm python-3.10.3-macos11.pkg
echo "Python succesfully installed"


echo "Onboarding script ran successfully"
