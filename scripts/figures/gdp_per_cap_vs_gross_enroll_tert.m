% plot gwp per capita and gross enrollment rate
ts = 2021-1969;
reg = 5;
reg_n = {"Africa","AsiaPacific",'EastEu',"LAC","WestEu"};
gender_n = {"Male","Female"};
fs = 12;
lw = 1.2;
colormap = [0.91,0.39,0.30;0.97,0.65,0.16;0.91,0.85,0.27;...
    0.47,0.67,0.19; 0.35,0.82,0.73;0.25,0.56,0.69;...
    0.40,0.42,0.80;0.76,0.30,0.78; 0.85,0.33,0.56;...
    0.77,0.21,0.35;0.82,0.30,0.33];

figure
for m =1:reg
    subplot(3,2,m)
    x_m = gwp_per_cap(m,:);
    y_m = net_enroll((m-1)*2+(1:2),:);

    [temp_value,temp_order] = sort(x_m);
    
    for sex = 1:2
        plot(x_m(temp_order),y_m(sex,temp_order),"Color",colormap(sex*2+1,:),"LineWidth",lw);
        hold on
    end
    
    ax=gca;
    ax.YLim=[0 1];
    ax.YTick=0:0.2:1;
    ax.YTickLabel={'0','0.2','0.4','0.6','0.8','1.0'};
    ax.XColor = 'black';
    ax.YColor = 'black';
    ylabel('% gross');
    ax.LineWidth=lw;
    title(reg_n{m});
    ax.FontSize=fs;
    if m == 1
        legend({'Male','Female'},'EdgeColor','none','FontSize',fs,...
            'Location','northwest');
    end
end
cd('C:\Users\VN73VS\OneDrive - Aalborg Universitet\Research Postdoc RU\Felix-Model')
fig = gcf;
fig.PaperUnits = 'inches';
fig.PaperPosition = [0 0 6 6];
print('gross_enroll_tert_by_reg','-dpng','-r300')



figure
for sex =1:2
    subplot(2,1,sex)
    for m = 1:reg
        x_m = gwp_per_cap(m,:);
        y_m = net_enroll((m-1)*2+sex,:);

        [temp_value,temp_order] = sort(x_m);

        plot(x_m(temp_order),y_m(temp_order),"Color",colormap(m,:),"LineWidth",lw);
        hold on
    end
    
    ax=gca;
    ax.YLim=[0 1];
    ax.YTick=0:0.2:1;
    ax.YTickLabel={'0','0.2','0.4','0.6','0.8','1.0'};
    ax.XColor = 'black';
    ax.YColor = 'black';
    ylabel('% gross');
    ax.LineWidth=lw;
    title(gender_n{sex});
    ax.FontSize=fs;
    if sex == 1
        legend(reg_n,'EdgeColor','none','FontSize',fs,...
            'Location','northwest',"NumColumns",3,"Color","None");
    end
end
cd('C:\Users\VN73VS\OneDrive - Aalborg Universitet\Research Postdoc RU\Felix-Model')
fig = gcf;
fig.PaperUnits = 'inches';
fig.PaperPosition = [0 0 6 6];
print('gross_enroll_tert_by_sex','-dpng','-r300')