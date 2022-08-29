echo "Downloading python 3.10.3"
curl https://www.python.org/ftp/python/3.10.3/python-3.10.3-macos11.pkg >> python-3.10.3-macos11.pkg

echo "Installing python 3.10.3"
sudo installer -pkg python-3.10.3-macos11.pkg -target /

echo "Setting up SSL links for your new version of python"
ln -s /etc/ssl/* /Library/Frameworks/Python.framework/Versions/3.10/etc/openssl

echo "Setting up path links for your new version of python"
export PATH="$HOME/Library/Python/3.10/bin":"$PATH" >> ~/.zshrc
# If you come from bash you might have to change your $PATH.
export PATH=$HOME/bin:/usr/local/bin:$PATH >> ~/.zshrc
export PATH="$HOME/.poetry/bin:$PATH" >> ~/.zshrc

rm python-3.10.3-macos11.pkg
echo "Python succesfully installed"

## Install homebrew
## Check if exists
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
echo "Homebrew successfully installed"

echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> $HOME/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"

echo "Adding homebrew link for m1 macs"
echo "export PATH=$PATH:/opt/homebrew/bin" >> ~/.zshrc

echo "Adding path fix to zshrc"
echo "export if ! [[ ""$PATH"" =~ ""$HOME/.local/bin:$HOME/bin:"" ]] then PATH=""$HOME/.local/bin:$HOME/bin:$PATH"" fi export PATH" >> ~/.zshrc

## install git
echo "Installing git.."
brew install git
echo "git successfully installed"

## Get oh my zsh (plugins, themes for zsh).
sh -c "$(curl -fsSL https://raw.github.com/robbyrussell/oh-my-zsh/master/tools/install.sh)" "" --unattended
git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions
## Set zsh theme
sed -i '' 's/ZSH_THEME=".*"/ZSH_THEME="bira"/g' ~/.zshrc
sed -i '' 's/plugins=(git)/plugins=(git zsh-autosuggestions jump)/g' ~/.zshrc

## Fix zsh permissions
chmod 755 /usr/local/share/zsh
chmod 755 /usr/local/share/zsh/site-functions

## source file to get jump working
source ~/.zshrc

## install the project
echo "Installing the analytics project.."
mkdir ~/repos/
cd ~/repos/
git clone git@gitlab.com:gitlab-data/analytics.git
cd analytics
mark analytics
echo "Analytics repo successfully installed"

## you can now type "jump analytics" and you're in the right place

## gl_open is now an alias to open this on gitlab.com

echo "Setting up dbt profile.."
mkdir ~/.dbt
touch ~/.dbt/profiles.yml
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/sample_profiles.yml >> ~/.dbt/profiles.yml
echo "dbt profile created.. You will need to edit this file later."

## you will need to edit this file
## install visual studio code
echo "Installing VS Code.."
brew install --cask visual-studio-code
## this might ask you for your password
code --version
echo "VS Code successfully installed"

## install miniforge
echo "Installing miniforge.."
brew install miniforge
echo "export PATH=/usr/local/mambaforge/bin:"$PATH"" >> ~/.bash_profile
echo "export PATH=/usr/local/mambaforge/bin:"$PATH"" >> ~/.zshrc
echo "miniforge installed succesfully"

## Set up the computer to contribute to the handbook
echo "Setting up your computer to contribute to the handbook..."
cd ~/repos/
git clone git@gitlab.com:gitlab-com/www-gitlab-com.git
echo "Handbook project successfully installed"

echo "You've got everything set to build the handbook locally."
echo "Setting up jump for the handbook.."
cd www-gitlab-com/
mark handbook
echo "handbook jump alias successfully added"

## install iterm2
echo "Installing iTerm2.."
cd ~/Downloads
curl https://iterm2.com/downloads/stable/iTerm2-3_4_16.zip > iTerm2.zip
unzip iTerm2.zip - A &> /dev/null
mv iTerm.app/ /Applications/iTerm.app
spctl --add /Applications/iTerm.app
rm -rf iTerm2.zip
echo "iTerm2 successfully installed.. Adding colors.."

cd ~/Downloads
mkdir -p ${HOME}/iterm2-colors
cd ${HOME}/iterm2-colors
curl -L https://github.com/mbadolato/iTerm2-Color-Schemes/zipball/master > iterm2-colors.zip
unzip iterm2-colors.zip
rm iterm2-colors.zip
echo "iTerm2 + Colors installed"

echo "Adding completions"

## install the dbt completion script
curl https://raw.githubusercontent.com/fishtown-analytics/dbt-completion.bash/master/dbt-completion.bash > ~/.dbt-completion.bash
echo 'autoload -U +X compinit && compinit' >> ~/.zshrc
echo 'autoload -U +X bashcompinit && bashcompinit' >> ~/.zshrc
echo 'source ~/.dbt-completion.bash' >> ~/.zshrc

## create global gitignore
echo "Creating a global gitignore.."
git config --global core.excludesfile ~/.gitignore
touch ~/.gitignore
echo '.DS_Store' >> ~/.gitignore
echo '.idea' >> ~/.gitignore
echo "Global gitignore created"


## Add in helper script
echo "Copying make life easier script.."
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/make_life_easier.zsh > ~/.make_life_easier.zsh
echo 'source ~/.make_life_easier.zsh' >> ~/.zshrc
echo "Copied successfully"


echo "export SNOWFLAKE_TRANSFORM_WAREHOUSE=ANALYST_XS" >> ~/.zshrc
echo "export SNOWFLAKE_LOAD_DATABASE=RAW" >> ~/.zshrc
echo "export SNOWFLAKE_SNAPSHOT_DATABASE='SNOWFLAKE'" >> ~/.zshrc
echo 'export PATH="/usr/local/opt/gettext/bin:$PATH"' >> ~/.zshrc
echo 'export RUBY_CONFIGURE_OPTS="--with-openssl-dir=$(brew --prefix openssl@1.1)"' >> ~/.zshrc
echo 'setopt nomatch' >> ~/.zshrc


echo "Onboarding script ran successfully"
